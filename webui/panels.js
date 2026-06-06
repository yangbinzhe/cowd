window.Panels = (()=>{
  const cont=()=>UI.$('panel-content');

  function fmtPct(value){
    return typeof value === 'number' ? Math.round(value * 100) + '%' : 'n/a';
  }

  function fmtNumber(value){
    return value === undefined || value === null ? 0 : value;
  }

  function memoryHealthClass(status){
    if(!status)return 'memory-badge';
    if(status.degraded || status.status === 'degraded')return 'memory-badge warn';
    if(status.enabled === false || status.status === 'disabled')return 'memory-badge danger';
    return 'memory-badge ok';
  }

  function renderMemoryMetric(label,value,sub){
    const item=UI.el('div','memory-metric');
    item.innerHTML='<b>'+UI.esc(String(value))+'</b><small>'+UI.esc(label)+'</small>'+(sub?'<em>'+UI.esc(sub)+'</em>':'');
    return item;
  }

  function renderMemoryPacket(packetResponse,target){
    target.innerHTML='';
    const packet=packetResponse && packetResponse.packet;
    const card=UI.el('div','memory-card memory-packet');
    card.innerHTML='<h3>Context Packet</h3>';
    if(!packet || !Array.isArray(packet.selected) || packet.selected.length===0){
      card.appendChild(UI.el('div','panel-empty','No active packet'));
      target.appendChild(card);
      return;
    }
    const meta=UI.el('div','memory-card-meta');
    meta.textContent='tokens '+(packet.token_estimate||0)+(packet.truncated?' · truncated':'')+' · selected '+packet.selected.length+' · omitted '+((packet.omitted||[]).length);
    card.appendChild(meta);
    packet.selected.slice(0,10).forEach(function(item){
      const atom=item.atom||{};
      const row=UI.el('div','memory-packet-row');
      const role=UI.el('span','memory-role '+String(item.role||'').toLowerCase());
      role.textContent=item.role||'Memory';
      const body=UI.el('div','memory-packet-body');
      const title=atom.title||atom.content||atom.id||'Memory';
      body.innerHTML='<b>'+UI.esc(String(title).slice(0,120))+'</b><small>'+UI.esc([atom.layer,atom.category,atom.state].filter(Boolean).join(' · '))+'</small><em>'+UI.esc(item.reason||'selected')+'</em>';
      row.appendChild(role);
      row.appendChild(body);
      card.appendChild(row);
    });
    if((packet.omitted||[]).length){
      const omitted=UI.el('div','memory-omitted');
      omitted.textContent='Omitted: '+packet.omitted.slice(0,3).map(o=>(o.title||o.id||'memory')+' ('+(o.reason||'bounded')+')').join(' · ');
      card.appendChild(omitted);
    }
    target.appendChild(card);
  }

  function shortHash(value){
    return String(value||'').slice(0,12)||'n/a';
  }

  function fmtPressure(bp){
    if(typeof bp !== 'number')return 'n/a';
    return Math.round(bp/100)+'%';
  }

  function contextTextPreview(value){
    return String(value||'').replace(/\s+/g,' ').trim().slice(0,180);
  }

  function renderContextItem(item){
    const row=UI.el('div','context-item');
    const role=UI.el('span','context-role '+String(item.role||'').toLowerCase());
    role.textContent=item.role||item.source||'item';
    const body=UI.el('div','context-item-body');
    body.innerHTML='<b>'+UI.esc(contextTextPreview(item.content||item.id||'context item'))+'</b><small>'+UI.esc([item.source,item.authority,item.visibility].filter(Boolean).join(' · '))+'</small><em>score '+UI.esc(typeof item.score==='number'?item.score.toFixed(2):'n/a')+' · '+UI.esc(item.token_estimate||0)+' tk</em>';
    row.appendChild(role);
    row.appendChild(body);
    return row;
  }

  function renderContextSegment(label,items){
    const sec=UI.el('div','context-segment');
    sec.innerHTML='<h4>'+UI.esc(label)+'</h4>';
    const list=Array.isArray(items)?items:[];
    if(!list.length){
      sec.appendChild(UI.el('div','panel-empty','empty'));
      return sec;
    }
    list.slice(0,4).forEach(function(text){
      const pre=UI.el('pre');
      pre.textContent=contextTextPreview(text);
      sec.appendChild(pre);
    });
    return sec;
  }

  function renderContextHistoryItem(item){
    const row=UI.el('button','context-history-item');
    const envelope=item.envelope||{};
    const diagnostics=envelope.diagnostics||{};
    const stamp=item.created_at_ms?new Date(item.created_at_ms).toLocaleTimeString():'n/a';
    row.type='button';
    row.innerHTML='<b>'+UI.esc(envelope.profile||'Context')+'</b><small>'+UI.esc([item.envelope_id||envelope.id||'no-id','seq '+(item.sequence??'n/a'),stamp].join(' · '))+'</small><em>'+UI.esc(contextTextPreview(envelope.intent||''))+'</em><span>'+UI.esc(fmtPressure(diagnostics.pressure_bp))+'</span>';
    row.onclick=function(){
      const next=(item.envelope)||{};
      const evt=new CustomEvent('cowd-context-history-selected',{detail:next});
      window.dispatchEvent(evt);
    };
    return row;
  }

  async function renderContext(){
    const c=cont();c.innerHTML='';
    const hdr=UI.el('div','panel-section context-header');
    hdr.innerHTML='<h3>Context Runtime</h3>';
    const controls=UI.el('div','context-controls');
    const input=UI.el('input');
    input.placeholder='Inspect intent...';
    const refresh=UI.el('button','btn-secondary btn-sm');
    refresh.textContent='Refresh';
    controls.appendChild(input);
    controls.appendChild(refresh);
    hdr.appendChild(controls);
    c.appendChild(hdr);

    const mount=UI.el('div');
    c.appendChild(mount);
    async function load(){
      mount.innerHTML='';
      const opts={q:input.value||''};
      if(Api.sid)opts.session_id=Api.sid;
      try{
        const response=await Api.currentContext(opts);
        const envelope=(response&&response.envelope)||{};
        const diagnostics=envelope.diagnostics||{};
        const budget=envelope.budget||{};
        const assembled=envelope.assembled||{};

        const overview=UI.el('div','panel-section context-overview');
        const source=response.source||'synthetic';
        overview.innerHTML='<div class="context-title"><b>'+UI.esc(envelope.profile||'MainTurn')+'</b><span>'+UI.esc(source)+'</span></div>';
        const metrics=UI.el('div','memory-metrics');
        metrics.appendChild(renderMemoryMetric('pressure',fmtPressure(diagnostics.pressure_bp),'context'));
        metrics.appendChild(renderMemoryMetric('used',budget.used_tokens||0,'tokens'));
        metrics.appendChild(renderMemoryMetric('total',budget.total_tokens||0,'tokens'));
        metrics.appendChild(renderMemoryMetric('stable',shortHash(diagnostics.stable_head_hash),'hash'));
        metrics.appendChild(renderMemoryMetric('runtime',shortHash(diagnostics.runtime_header_hash),'hash'));
        metrics.appendChild(renderMemoryMetric('dynamic',shortHash(diagnostics.dynamic_tail_hash),'hash'));
        overview.appendChild(metrics);
        if((diagnostics.degraded_sources||[]).length){
          const degraded=UI.el('div','context-degraded');
          degraded.textContent='degraded: '+diagnostics.degraded_sources.join(', ');
          overview.appendChild(degraded);
        }
        if((diagnostics.recommendations||[]).length){
          const recs=UI.el('div','context-recommendations');
          recs.innerHTML='<h4>Recommendations</h4>';
          diagnostics.recommendations.slice(0,4).forEach(function(text){
            const row=UI.el('div','context-recommendation');
            const label=UI.el('span');
            label.textContent=text;
            row.appendChild(label);
            if(Api.sid && envelope.id){
              const ack=UI.el('button','btn-secondary btn-xs');
              ack.type='button';
              ack.textContent='Ack';
              ack.onclick=async function(){
                try{
                  await Api.recordContextRecommendation(Api.sid,{
                    envelope_id:envelope.id,
                    recommendation:text,
                    action:'acknowledged'
                  });
                  ack.textContent='Done';
                  ack.disabled=true;
                }catch(e){UI.showToast(e.message,'error')}
              };
              row.appendChild(ack);
            }
            recs.appendChild(row);
          });
          overview.appendChild(recs);
        }
        mount.appendChild(overview);

        const selected=UI.el('div','panel-section context-list');
        selected.innerHTML='<h3>Selected Context</h3>';
        const items=envelope.selected||[];
        if(!items.length)selected.appendChild(UI.el('div','panel-empty','No selected context'));
        items.slice(0,12).forEach(function(item){selected.appendChild(renderContextItem(item))});
        mount.appendChild(selected);

        const omitted=UI.el('div','panel-section context-list');
        omitted.innerHTML='<h3>Omitted Context</h3>';
        const omittedItems=envelope.omitted||[];
        if(!omittedItems.length)omitted.appendChild(UI.el('div','panel-empty','No omitted context'));
        omittedItems.slice(0,8).forEach(function(item){
          const row=UI.el('div','context-omission');
          row.textContent=(item.source||'context')+' · '+(item.reason||'omitted')+' · '+(item.token_estimate||0)+' tk';
          omitted.appendChild(row);
        });
        mount.appendChild(omitted);

        const segments=UI.el('div','panel-section context-segments');
        segments.innerHTML='<h3>Prompt Segments</h3>';
        segments.appendChild(renderContextSegment('stable head',assembled.stable_head));
        segments.appendChild(renderContextSegment('runtime header',assembled.runtime_header));
        segments.appendChild(renderContextSegment('dynamic tail',assembled.dynamic_tail));
        mount.appendChild(segments);

        const history=UI.el('div','panel-section context-history');
        history.innerHTML='<h3>Context Timeline</h3>';
        if(!Api.sid){
          history.appendChild(UI.el('div','panel-empty','No active session'));
        }else{
          try{
            const timeline=await Api.contextHistory(Api.sid,{limit:8});
            const rows=timeline.envelopes||[];
            if(!rows.length)history.appendChild(UI.el('div','panel-empty','No persisted envelopes'));
            rows.slice(-8).reverse().forEach(function(item){
              history.appendChild(renderContextHistoryItem(item));
            });
          }catch(historyError){
            history.appendChild(UI.el('div','panel-empty','Context timeline unavailable'));
          }
        }
        mount.appendChild(history);
      }catch(e){
        mount.appendChild(UI.el('div','panel-empty','Context unavailable: '+e.message));
      }
    }
    refresh.onclick=load;
    input.onkeydown=function(e){if(e.key==='Enter')load()};
    load();
  }

  function renderMemoryLinks(linksResponse,target){
    target.innerHTML='';
    const links=(linksResponse && (linksResponse.links||linksResponse)) || [];
    const sec=UI.el('div','memory-card');
    sec.innerHTML='<h3>Memory Links</h3>';
    if(!links.length){
      sec.appendChild(UI.el('div','panel-empty','No links'));
      target.appendChild(sec);
      return;
    }
    const counts={};
    links.forEach(l=>{const k=l.kind||'Link';counts[k]=(counts[k]||0)+1});
    const summary=UI.el('div','memory-link-kinds');
    Object.keys(counts).sort().forEach(function(kind){
      const chip=UI.el('span','memory-chip');
      chip.textContent=kind+' '+counts[kind];
      summary.appendChild(chip);
    });
    sec.appendChild(summary);
    links.slice(0,8).forEach(function(link){
      const row=UI.el('div','memory-link-row');
      row.innerHTML='<b>'+UI.esc(String(link.kind||'Link'))+'</b><small>'+UI.esc(String(link.from||'').slice(0,8))+' → '+UI.esc(String(link.to||'').slice(0,8))+' · '+(typeof link.weight==='number'?link.weight.toFixed(2):'n/a')+'</small><em>'+UI.esc(link.evidence||'')+'</em>';
      sec.appendChild(row);
    });
    target.appendChild(sec);
  }

  function addNetworkNode(nodes,id,label,type){
    const key=String(id||label||'node');
    if(!nodes.has(key))nodes.set(key,{id:key,label:String(label||key),type:type||'memory'});
    return key;
  }

  function buildKnowledgeNetwork(triples,links){
    const nodes=new Map();
    const edges=[];
    (triples||[]).slice(0,30).forEach(function(t){
      const s=addNetworkNode(nodes,t.subject||t.s,t.subject||t.s,'entity');
      const o=addNetworkNode(nodes,t.object||t.o,t.object||t.o,'entity');
      edges.push({from:s,to:o,label:String(t.predicate||t.p||'relates'),type:'triple',weight:1});
    });
    (links||[]).slice(0,40).forEach(function(l){
      const from=addNetworkNode(nodes,l.from,String(l.from||'').slice(0,8),'memory');
      const to=addNetworkNode(nodes,l.to,String(l.to||'').slice(0,8),'memory');
      edges.push({from,to,label:String(l.kind||'Link'),type:'link',weight:typeof l.weight==='number'?l.weight:0.5,evidence:l.evidence||''});
    });
    return {nodes:[...nodes.values()].slice(0,48),edges};
  }

  function renderKnowledgeGraph(data,target){
    target.innerHTML='';
    const wrap=UI.el('div','memory-network');
    const width=316,height=260,cx=width/2,cy=height/2;
    const ns='http://www.w3.org/2000/svg';
    const controls=UI.el('div','memory-network-controls');
    const search=UI.el('input');
    search.placeholder='Filter network...';
    const type=UI.el('select');
    type.innerHTML='<option value="all">All relations</option><option value="triple">Triples</option><option value="link">Memory links</option>';
    const reset=UI.el('button','btn-secondary btn-sm');
    reset.textContent='Reset';
    controls.appendChild(search);
    controls.appendChild(type);
    controls.appendChild(reset);
    wrap.appendChild(controls);

    const svg=document.createElementNS(ns,'svg');
    svg.setAttribute('viewBox','0 0 '+width+' '+height);
    svg.setAttribute('role','img');
    svg.setAttribute('aria-label','Knowledge network');
    const nodes=data.nodes||[];
    const edges=data.edges||[];
    const detail=UI.el('div','memory-network-detail');
    if(!nodes.length){
      wrap.appendChild(UI.el('div','panel-empty','No network'));
      target.appendChild(wrap);
      return;
    }
    const nodeById=new Map(nodes.map(n=>[n.id,n]));
    let selectedId=null;

    function nodeText(id){
      const n=nodeById.get(id);
      return ((n&&n.label)||id||'').toLowerCase();
    }

    function matchesQuery(edge,q){
      if(!q)return true;
      return nodeText(edge.from).includes(q)
        || nodeText(edge.to).includes(q)
        || String(edge.label||'').toLowerCase().includes(q)
        || String(edge.evidence||'').toLowerCase().includes(q);
    }

    function renderDetail(visibleEdges){
      const node=selectedId&&nodeById.get(selectedId);
      if(!node){
        detail.innerHTML='<b>Network</b><small>'+nodes.length+' nodes · '+visibleEdges.length+' visible relations</small>';
        return;
      }
      const connected=visibleEdges.filter(e=>e.from===selectedId||e.to===selectedId);
      detail.innerHTML='<b>'+UI.esc(node.label)+'</b><small>'+UI.esc(node.type)+' · '+connected.length+' visible connections</small>';
      connected.slice(0,6).forEach(function(edge){
        const other=edge.from===selectedId?edge.to:edge.from;
        const row=UI.el('em');
        row.textContent=edge.label+' -> '+((nodeById.get(other)||{}).label||String(other).slice(0,8));
        detail.appendChild(row);
      });
    }

    function draw(){
      svg.innerHTML='';
      const q=search.value.trim().toLowerCase();
      const selectedType=type.value;
      const visibleEdges=edges.filter(edge=>(selectedType==='all'||edge.type===selectedType)&&matchesQuery(edge,q));
      const visibleNodeIds=new Set();
      visibleEdges.forEach(edge=>{visibleNodeIds.add(edge.from);visibleNodeIds.add(edge.to)});
      if(!visibleEdges.length&&!q){
        nodes.forEach(node=>visibleNodeIds.add(node.id));
      }
      if(q){
        nodes.forEach(node=>{
          if(String(node.label||node.id).toLowerCase().includes(q))visibleNodeIds.add(node.id);
        });
      }
      const visibleNodes=nodes.filter(node=>visibleNodeIds.has(node.id));
      const pos={};
      const radius=Math.min(104,Math.max(56,28+visibleNodes.length*3));
      visibleNodes.forEach(function(node,i){
        const angle=(-Math.PI/2)+(Math.PI*2*i/Math.max(visibleNodes.length,1));
        pos[node.id]={x:cx+Math.cos(angle)*radius,y:cy+Math.sin(angle)*radius};
      });
      const connectedToSelected=new Set();
      if(selectedId){
        visibleEdges.forEach(edge=>{
          if(edge.from===selectedId)connectedToSelected.add(edge.to);
          if(edge.to===selectedId)connectedToSelected.add(edge.from);
        });
      }
      visibleEdges.forEach(function(edge){
        const a=pos[edge.from],b=pos[edge.to];
        if(!a||!b)return;
        const active=!selectedId||edge.from===selectedId||edge.to===selectedId;
        const line=document.createElementNS(ns,'line');
        line.setAttribute('x1',a.x);line.setAttribute('y1',a.y);line.setAttribute('x2',b.x);line.setAttribute('y2',b.y);
        line.setAttribute('class','memory-edge '+edge.type+(active?' active':' dim'));
        line.setAttribute('stroke-width',String(1+Math.min(2,edge.weight||0)));
        svg.appendChild(line);
        const label=document.createElementNS(ns,'text');
        label.setAttribute('x',(a.x+b.x)/2);label.setAttribute('y',(a.y+b.y)/2-3);
        label.setAttribute('class','memory-edge-label'+(active?' active':' dim'));
        label.textContent=edge.label;
        svg.appendChild(label);
      });
      visibleNodes.forEach(function(node){
        const p=pos[node.id];
        const group=document.createElementNS(ns,'g');
        const active=!selectedId||node.id===selectedId||connectedToSelected.has(node.id);
        const matched=q&&String(node.label||node.id).toLowerCase().includes(q);
        group.setAttribute('class','memory-node '+node.type+(active?' active':' dim')+(node.id===selectedId?' selected':'')+(matched?' matched':''));
        group.setAttribute('data-node-id',node.id);
        group.onclick=function(){selectedId=selectedId===node.id?null:node.id;draw()};
        const circle=document.createElementNS(ns,'circle');
        circle.setAttribute('cx',p.x);circle.setAttribute('cy',p.y);circle.setAttribute('r',node.type==='entity'?13:11);
        const text=document.createElementNS(ns,'text');
        text.setAttribute('x',p.x);text.setAttribute('y',p.y+28);
        text.setAttribute('class','memory-node-label');
        text.textContent=node.label.length>14?node.label.slice(0,13)+'...':node.label;
        group.appendChild(circle);
        group.appendChild(text);
        svg.appendChild(group);
      });
      if(!visibleNodes.length){
        const empty=document.createElementNS(ns,'text');
        empty.setAttribute('x',cx);empty.setAttribute('y',cy);
        empty.setAttribute('class','memory-node-label');
        empty.textContent='No matching relations';
        svg.appendChild(empty);
      }
      renderDetail(visibleEdges);
    }

    search.oninput=draw;
    type.onchange=function(){selectedId=null;draw()};
    reset.onclick=function(){search.value='';type.value='all';selectedId=null;draw()};
    wrap.appendChild(svg);
    const legend=UI.el('div','memory-network-legend');
    legend.innerHTML='<span>entity</span><span>memory</span><span>triple</span><span>link</span>';
    wrap.appendChild(legend);
    wrap.appendChild(detail);
    target.appendChild(wrap);
    draw();
  }

  async function renderMemory(){
    const c=cont();c.innerHTML='';
    const hdr=UI.el('div','panel-section');
    hdr.innerHTML='<h3>Memory</h3>';
    // P2-09: Spatial memory toggle — Wing/Room/Drawer visualization
    const spatialBtn=UI.el('button');
    spatialBtn.textContent='Spatial View';
    spatialBtn.style.cssText='font-size:11px;margin-left:8px;padding:2px 6px;';
    spatialBtn.onclick=()=>renderMemorySpatial();
    hdr.appendChild(spatialBtn);
    const networkBtn=UI.el('button');
    networkBtn.textContent='Network';
    networkBtn.style.cssText='font-size:11px;margin-left:6px;padding:2px 6px;';
    networkBtn.onclick=()=>renderMemoryNetwork();
    hdr.appendChild(networkBtn);
    const search=UI.el('input');
    search.placeholder='Search memory...';
    search.oninput=function(){const q=this.value;if(q.length>1)doMemorySearch(q)};
    hdr.appendChild(search);
    c.appendChild(hdr);

    const stats=UI.el('div','panel-section memory-overview');
    c.appendChild(stats);
    try{
      const status=await Api.memoryStatus();
      const s=await Api.memoryStats();
      const label=status.status||((status.enabled)?'ready':'disabled');
      const reason=status.degraded_reason||status.message||'';
      const kh=status.kernel_health||{};
      const header=UI.el('div','memory-overview-head');
      header.innerHTML='<span class="'+memoryHealthClass(status)+'">'+UI.esc(label)+'</span>'+(reason?'<small>'+UI.esc(reason)+'</small>':'');
      stats.appendChild(header);
      const grid=UI.el('div','memory-metrics');
      grid.appendChild(renderMemoryMetric('entries',fmtNumber(s.total_entries||s.count)));
      grid.appendChild(renderMemoryMetric('entities',fmtNumber(s.entity_count||s.entities)));
      grid.appendChild(renderMemoryMetric('triples',fmtNumber(s.triple_count||s.triples)));
      grid.appendChild(renderMemoryMetric('evidence',fmtPct(kh.evidence_coverage),'coverage'));
      grid.appendChild(renderMemoryMetric('links',fmtPct(kh.link_coverage),'coverage'));
      grid.appendChild(renderMemoryMetric('lag',fmtNumber(kh.background_lag_ms)+'ms','background'));
      stats.appendChild(grid);
    }catch(e){stats.textContent='Stats unavailable'}

    const kernelSec=UI.el('div','memory-grid');
    const packetSec=UI.el('div');
    const linksSec=UI.el('div');
    kernelSec.appendChild(packetSec);
    kernelSec.appendChild(linksSec);
    c.appendChild(kernelSec);
    renderMemoryPacket(null,packetSec);
    try{renderMemoryLinks(await Api.memoryLinks(),linksSec)}catch(e){renderMemoryLinks({links:[]},linksSec)}

    const entitySec=UI.el('div','panel-section');
    entitySec.innerHTML='<h3>Entities</h3>';
    c.appendChild(entitySec);
    try{
      const entities=await Api.listEntities();
      (entities||[]).slice(0,10).forEach(function(e){
        var item=UI.el('div','panel-item');
        item.textContent=(e.name||e.entity||e);
        entitySec.appendChild(item);
      });
    }catch(e){entitySec.appendChild(UI.el('div','panel-empty','No entities'))}

    var tripleSec=UI.el('div','panel-section');
    tripleSec.innerHTML='<h3>Knowledge Triples</h3>';
    c.appendChild(tripleSec);
    try{
      var triples=await Api.listTriples();
      (triples||[]).slice(0,10).forEach(function(t){
        var item=UI.el('div','panel-item');
        item.textContent=(t.subject||t.s||'')+' → '+(t.predicate||t.p||'')+' → '+(t.object||t.o||'');
        tripleSec.appendChild(item);
      });
    }catch(e){tripleSec.appendChild(UI.el('div','panel-empty','No triples'))}

    const symbolSec=UI.el('div','panel-section');
    symbolSec.innerHTML='<h3>Symbol Links</h3>';
    const symbolInput=UI.el('input');
    symbolInput.id='memory-symbol-search';
    symbolInput.placeholder='Find by symbol...';
    const symbolResults=UI.el('div');
    symbolResults.id='memory-symbol-results';
    symbolInput.oninput=function(){renderMemorySymbolResults(this.value,symbolResults)};
    symbolSec.appendChild(symbolInput);
    symbolSec.appendChild(symbolResults);
    c.appendChild(symbolSec);

    const layers=UI.el('div','panel-section');
    layers.innerHTML='<h3>Layers</h3>';
    c.appendChild(layers);
    try{
      const ls=await Api.listMemoryLayers();
      (ls.layers||ls||[]).forEach(l=>{
        const layerName = l.name || l.layer || l.id || l;
        const item=UI.el('div','panel-item');
        const name=UI.el('span','pi-name');
        name.textContent=layerName;
        item.appendChild(name);
        item.onclick=()=>renderMemoryLayer(layerName);
        layers.appendChild(item);
      });
    }catch(e){layers.appendChild(UI.el('div','panel-empty','No layers'))}
  }

  async function renderMemoryLayer(layer){
    const c=cont();c.innerHTML='';
    c.appendChild(UI.el('div','panel-section','<h3>'+UI.esc(layer)+'</h3>'));
    const btn=UI.el('button','btn-primary');
    btn.textContent='+ Add Entry';
    btn.onclick=()=>showMemoryEntryForm(layer);
    c.appendChild(btn);
    try{
      const data=await Api.getMemoryLayer(layer);
      (data.entries||data||[]).slice(0,30).forEach(e=>{
        const item=UI.el('div','panel-item');
        const name=UI.el('span','pi-name');
        name.textContent=(e.content||e.text||e.name||'').slice(0,80);
        item.appendChild(name);
        item.onclick=()=>{UI.showToast(e.content||e.text||JSON.stringify(e))};
        c.appendChild(item);
      });
    }catch(e){c.appendChild(UI.el('div','panel-empty','Error: '+e.message))}
  }

  async function renderMemorySymbolResults(symbol,target){
    if(!target)return;
    target.innerHTML='';
    const q=(symbol||'').trim();
    if(q.length<2)return;
    try{
      const entries=await Api.findMemoriesBySymbol(q);
      if(!entries.length){
        target.appendChild(UI.el('div','panel-empty','No linked memories'));
        return;
      }
      entries.slice(0,12).forEach(function(e){
        const item=UI.el('div','panel-item memory-symbol-result');
        const name=UI.el('span','pi-name');
        const label=e.title||e.name||e.content||e.text||e.id||'Memory entry';
        name.textContent=String(label).slice(0,90);
        item.appendChild(name);
        item.onclick=function(){UI.showToast(e.content||e.text||e.title||JSON.stringify(e))};
        target.appendChild(item);
      });
    }catch(e){
      target.appendChild(UI.el('div','panel-empty','Error: '+e.message));
    }
  }

  function showMemoryEntryForm(layer){
    const c=cont();
    const sec=UI.el('div','panel-section');
    sec.innerHTML='<h3>New Entry ('+UI.esc(layer)+')</h3>';
    const ta=UI.el('textarea');
    ta.rows=4;ta.placeholder='Entry content...';
    const btn=UI.el('button','btn-primary');
    btn.textContent='Save';
    btn.onclick=async()=>{
      try{await Api.createMemoryEntry(layer,{content:ta.value});UI.showToast('Saved','success');renderMemoryLayer(layer)}catch(e){UI.showToast(e.message,'error')}
    };
    sec.appendChild(ta);sec.appendChild(btn);
    c.insertBefore(sec,c.firstChild);
  }

  async function doMemorySearch(q){
    const c=cont();
    const sec=c.querySelector('.panel-section:last-child');
    if(!sec)return;
    try{
      const packetTarget=c.querySelector('.memory-grid > div:first-child');
      if(packetTarget){
        try{renderMemoryPacket(await Api.memoryPacket(q,{max_items:8,max_tokens:2000}),packetTarget)}catch(e){}
      }
      const r=await Api.recallExplain(q,20);
      const results=r.results||r||[];
      const list=UI.el('div');
      list.innerHTML='<h3>Recall Explain</h3>';
      if(r.mode||r.degraded_reason){
        const meta=UI.el('div','panel-item');
        meta.textContent='Mode: '+(r.mode||'unknown')+(r.degraded_reason?' · '+r.degraded_reason:'');
        list.appendChild(meta);
      }
      results.slice(0,20).forEach(e=>{
        const item=UI.el('div','panel-item');
        const source=[e.source_layer,e.category,e.mode].filter(Boolean).join(' · ');
        const score=typeof e.score==='number'?' · score '+e.score.toFixed(2):'';
        item.textContent=(source?source+score+' — ':'')+(e.snippet||e.content||e.text||'').slice(0,140);
        list.appendChild(item);
      });
      const old=c.querySelector('.search-results');
      if(old)old.remove();
      list.className='search-results panel-section';
      c.appendChild(list);
    }catch(e){}
  }

  async function renderSkills(){
    const c=cont();c.innerHTML='<h3>Skills</h3>';
    try{
      const data=await Api.listSkills();
      (data.skills||data||[]).forEach(s=>{
        const item=UI.el('div','panel-item');
        const name=UI.el('span','pi-name');
        name.textContent=s.name||s;
        item.appendChild(name);
        const acts=UI.el('span','pi-actions');
        if(!s.installed){
          const installBtn=UI.el('button');
          installBtn.textContent='Install';
          installBtn.onclick=async()=>{try{await Api.installSkill(s.name||s);UI.showToast('Installed','success');renderSkills()}catch(e){UI.showToast(e.message,'error')}};
          acts.appendChild(installBtn);
        }else{
          const uninstallBtn=UI.el('button');
          uninstallBtn.textContent='Remove';
          uninstallBtn.style.color='var(--error)';
          uninstallBtn.onclick=async()=>{try{await Api.uninstallSkill(s.name||s);UI.showToast('Removed','success');renderSkills()}catch(e){UI.showToast(e.message,'error')}};
          acts.appendChild(uninstallBtn);
        }
        item.appendChild(acts);
        c.appendChild(item);
      });
    }catch(e){c.appendChild(UI.el('div','panel-empty','No skills loaded'))}
  }

  async function renderCrons(){
    const c=cont();c.innerHTML='<h3>Crond Jobs</h3>';
    const btn=UI.el('button','btn-primary');
    btn.textContent='+ New Crond';
    btn.onclick=showCronForm;
    c.appendChild(btn);
    try{
      const data=await Api.listCrons();
      (data.crons||data||[]).forEach(cr=>{
        const item=UI.el('div','panel-item');
        const name=UI.el('span','pi-name');
        name.textContent=(cr.name||cr.id||'')+(cr.schedule?' ['+cr.schedule+']':'');
        item.appendChild(name);
        const acts=UI.el('span','pi-actions');
        const runBtn=UI.el('button');
        runBtn.textContent='Run';
        runBtn.onclick=async()=>{try{await Api.runCron(cr.id||cr.name);UI.showToast('Running','success')}catch(e){UI.showToast(e.message,'error')}};
        const delBtn=UI.el('button');
        delBtn.textContent='Del';
        delBtn.style.color='var(--error)';
        delBtn.onclick=async()=>{if(confirm('Delete this cron?')){try{await Api.deleteCron(cr.id||cr.name);renderCrons()}catch(e){UI.showToast(e.message,'error')}}};
        acts.appendChild(runBtn);acts.appendChild(delBtn);
        item.appendChild(acts);
        c.appendChild(item);
      });
    }catch(e){c.appendChild(UI.el('div','panel-empty','No crons'))}
  }

  function showCronForm(){
    const c=cont();
    const sec=UI.el('div','panel-section');
    sec.innerHTML='<h3>New Crond</h3>';
    const form=UI.el('div','panel-form');
    form.innerHTML='<input id="cron-name" placeholder="Name">';
    form.innerHTML+='<input id="cron-schedule" placeholder="Schedule (e.g. */5 * * * *)">';
    form.innerHTML+='<input id="cron-prompt" placeholder="Prompt to run">';
    form.innerHTML+='<select id="cron-model"><option>claude-sonnet-4-6</option><option>claude-haiku-4-5</option></select>';
    const btn=UI.el('button','btn-primary');
    btn.textContent='Create';
    btn.onclick=async()=>{
      const nm=UI.$('cron-name').value;
      const sch=UI.$('cron-schedule').value;
      const pr=UI.$('cron-prompt').value;
      const mod=UI.$('cron-model').value;
      try{await Api.createCron({name:nm,schedule:sch,prompt:pr,model:mod});UI.showToast('Crond created','success');renderCrons()}catch(e){UI.showToast(e.message,'error')}
    };
    sec.appendChild(form);sec.appendChild(btn);
    c.insertBefore(sec,c.firstChild);
  }

  async function renderSettings(){
    const c=cont();c.innerHTML='<h3>Settings</h3>';
    const themeSec=UI.el('div','panel-section');
    themeSec.innerHTML='<h3>Theme</h3>';
    const themeSel=UI.el('select');
    themeSel.innerHTML='<option value="dark">Dark</option><option value="light">Light</option><option value="system">System</option>';
    themeSel.value=localStorage.getItem('cowd-theme')||'dark';
    themeSel.onchange=function(){
      localStorage.setItem('cowd-theme',this.value);
      document.documentElement.dataset.theme=this.value;
      if(this.value==='system'){
        const pref=window.matchMedia('(prefers-color-scheme:dark)').matches?'dark':'light';
        document.documentElement.dataset.theme=pref;
      }
    };
    themeSec.appendChild(themeSel);
    c.appendChild(themeSec);

    const modelSec=UI.el('div','panel-section');
    modelSec.innerHTML='<h3>Default Model</h3>';
    try{
      const cfg=await Api.getConfig();
      modelSec.innerHTML+='<p style="font-size:12px;color:var(--text2)">Current: '+(cfg.model||'unknown')+'</p>';
    }catch(e){}
    c.appendChild(modelSec);

    const profileSec=UI.el('div','panel-section');
    profileSec.innerHTML='<h3>Profiles</h3>';
    const createRow=UI.el('div','profile-create-row');
    const input=UI.el('input');
    input.placeholder='New profile name';
    const createBtn=UI.el('button','btn-secondary');
    createBtn.textContent='Create';
    createBtn.onclick=async function(){
      const name=input.value.trim();
      if(!name)return;
      try{
        await Api.createProfile(name);
        UI.showToast('Profile created','success');
        renderSettings();
      }catch(e){UI.showToast(e.message,'error')}
    };
    createRow.appendChild(input);
    createRow.appendChild(createBtn);
    profileSec.appendChild(createRow);
    c.appendChild(profileSec);
    try{
      const data=await Api.listProfiles();
      const profiles=data.profiles||[];
      const runtime=data.runtime_profile||data.active_profile||'default';
      profiles.forEach(function(profile){
        const item=UI.el('div','panel-item profile-item');
        const name=UI.el('span','pi-name');
        name.textContent=(profile.name||profile.id)+(profile.id===runtime?' (runtime)':'');
        const actions=UI.el('span','pi-actions');
        if(profile.is_active){
          const active=UI.el('span','profile-active');
          active.textContent='active';
          actions.appendChild(active);
        }else{
          const switchBtn=UI.el('button');
          switchBtn.textContent='Switch';
          switchBtn.onclick=async function(){
            try{
              const r=await Api.switchProfile(profile.id);
              UI.showToast(r.restart_required?'Profile switch saved. Restart required.':'Profile switched','success');
              renderSettings();
            }catch(e){UI.showToast(e.message,'error')}
          };
          actions.appendChild(switchBtn);
        }
        if(profile.id!=='default'&&!profile.is_active){
          const deleteBtn=UI.el('button');
          deleteBtn.textContent='Delete';
          deleteBtn.style.color='var(--error)';
          deleteBtn.onclick=async function(){
            if(!confirm('Delete profile '+profile.id+'?'))return;
            try{await Api.deleteProfile(profile.id);renderSettings()}catch(e){UI.showToast(e.message,'error')}
          };
          actions.appendChild(deleteBtn);
        }
        item.appendChild(name);
        item.appendChild(actions);
        profileSec.appendChild(item);
      });
      if(data.active_profile&&data.active_profile!==runtime){
        const restart=UI.el('div','panel-empty');
        restart.textContent='Restart required to activate '+data.active_profile+' for memory and sessions.';
        profileSec.appendChild(restart);
      }
    }catch(e){
      profileSec.appendChild(UI.el('div','panel-empty','Profiles unavailable'));
    }
  }

  async function renderCCConfig(){
    const cc=UI.$('cc-content');cc.innerHTML='';
    try{
      const cfg=await Api.getConfig();
      const pre=UI.el('pre');
      pre.style.cssText='background:var(--bg);padding:12px;border-radius:var(--radius);font-size:12px;max-height:400px;overflow:auto';
      pre.textContent=JSON.stringify(cfg,null,2);
      cc.appendChild(pre);
    }catch(e){cc.textContent='Error: '+e.message}
  }

  async function renderCCProviders(){
    const cc=UI.$('cc-content');cc.innerHTML='';
    try{
      const providers=await Api.getProviders();
      const pre=UI.el('pre');
      pre.style.cssText='background:var(--bg);padding:12px;border-radius:var(--radius);font-size:12px;max-height:400px;overflow:auto';
      pre.textContent=JSON.stringify(providers,null,2);
      cc.appendChild(pre);
    }catch(e){cc.textContent='Error: '+e.message}
  }

  async function renderCCApproval(){
    const cc=UI.$('cc-content');cc.innerHTML='';
    try{
      const pend=await Api.pendingApprovals();
      cc.innerHTML='<div class="panel-section"><h3>Pending Approvals</h3></div>';
      (pend||[]).forEach(a=>{
        const item=UI.el('div','panel-item');
        item.innerHTML='<span class="pi-name">'+UI.esc(a.tool||a.action||a.id)+'</span>';
        const acts=UI.el('span','pi-actions');
        const approve=UI.el('button');
        approve.textContent='Approve';
        approve.style.color='var(--success)';
        approve.onclick=async()=>{try{await Api.respondApproval(a.id,true);renderCCApproval()}catch(e){}};
        const deny=UI.el('button');
        deny.textContent='Deny';
        deny.style.color='var(--error)';
        deny.onclick=async()=>{try{await Api.respondApproval(a.id,false);renderCCApproval()}catch(e){}};
        acts.appendChild(approve);acts.appendChild(deny);
        item.appendChild(acts);
        cc.appendChild(item);
      });
    }catch(e){cc.textContent='No pending approvals'}
  }

  async function renderCCHistory(){
    const cc=UI.$('cc-content');cc.innerHTML='';
    try{
      const hist=await Api.approvalHistory();
      cc.innerHTML='<div class="panel-section"><h3>Approval History</h3></div>';
      (hist||[]).slice(0,20).forEach(function(a){
        var item=UI.el('div','panel-item');
        item.innerHTML='<span class="pi-name">'+UI.esc(a.tool||a.action||a.id)+'</span><span style="font-size:11px;color:var(--text3)"> '+UI.esc(a.decision||a.status||'')+'</span>';
        cc.appendChild(item);
      });
    }catch(e){cc.textContent='No history'}
  }

  async function renderCCUsage(){
    const cc=UI.$('cc-content');cc.innerHTML='';
    try{
      const usage=await Api.getUsage();
      const pre=UI.el('pre');
      pre.style.cssText='background:var(--bg);padding:12px;border-radius:var(--radius);font-size:12px;max-height:400px;overflow:auto';
      pre.textContent=JSON.stringify(usage,null,2);
      cc.appendChild(pre);
    }catch(e){cc.textContent='Error: '+e.message}
  }

  async function renderAgents(){
    const c=cont();c.innerHTML='<h3>Agent Tasks</h3>';
    try{
      const tasks=await Api.taskStatus();
      const sec=UI.el('div','panel-section');
      sec.innerHTML='<h3>Task Registry</h3>';
      const current=tasks.current;
      if(current){
        const item=UI.el('div','panel-item');
        const body=UI.el('span','pi-name');
        body.textContent=(current.status||'running')+' · '+(current.objective||current.id);
        item.appendChild(body);
        sec.appendChild(item);
        const phase=current.phases&&current.phases.length
          ? current.phases[current.phases.length-1]
          : null;
        if(phase){
          const phaseBox=UI.el('div','panel-section');
          phaseBox.innerHTML='<h3>Current Phase</h3>';
          const title=UI.el('div','panel-item');
          title.textContent=(phase.status||'running')+' · '+(phase.name||phase.id)+' · '+(phase.objective||'');
          phaseBox.appendChild(title);
          (phase.acceptance||[]).slice(0,5).forEach(function(line){
            phaseBox.appendChild(UI.el('div','panel-empty','acceptance: '+line));
          });
          (phase.test_commands||[]).slice(0,5).forEach(function(cmd){
            phaseBox.appendChild(UI.el('div','panel-empty','test: '+cmd));
          });
          (phase.artifacts||[]).slice(-5).forEach(function(artifact){
            phaseBox.appendChild(UI.el('div','panel-empty',(artifact.kind||'artifact')+': '+(artifact.label||'')+' '+(artifact.value||'')));
          });
          if(phase.review_result){
            phaseBox.appendChild(UI.el('div','panel-empty','review: '+phase.review_result));
          }
          sec.appendChild(phaseBox);
        }
        if(current.blocker_reason){
          sec.appendChild(UI.el('div','panel-empty',current.blocker_reason));
        }
      }else{
        sec.appendChild(UI.el('div','panel-empty','No active task'));
      }
      (tasks.tasks||[]).slice(-8).reverse().forEach(function(task){
        if(current&&task.id===current.id)return;
        const row=UI.el('div','panel-item');
        const name=UI.el('span','pi-name');
        name.textContent=(task.status||'unknown')+' · '+(task.objective||task.id);
        row.appendChild(name);
        sec.appendChild(row);
      });
      c.appendChild(sec);
    }catch(e){c.appendChild(UI.el('div','panel-empty','Agents info unavailable'))}
  }

  async function renderTools(){
    const c=cont();c.innerHTML='<h3>Tool Execution</h3>';
    try{
      const cfg=await Api.getConfig();
      const sec=UI.el('div','panel-section');
      sec.innerHTML='<h3>Available Tools</h3>';
      const pre=UI.el('pre');
      pre.style.cssText='background:var(--bg);padding:12px;border-radius:var(--radius);font-size:12px;max-height:300px;overflow:auto';
      const toolNames=['bash','read','write','edit','glob','grep','lsp','webfetch','memory','skills','approval','files'];
      pre.textContent=toolNames.map(t=>'• '+t).join('\n');
      sec.appendChild(pre);
      c.appendChild(sec);
      const histSec=UI.el('div','panel-section');
      histSec.innerHTML='<h3>Execution History</h3>';
      histSec.appendChild(UI.el('div','panel-empty','Tool executions appear in the SSE stream.\nEach tool_start/tool_complete event is rendered inline in chat as a ToolCard.\nThis panel shows the tool registry and configuration.'));
      c.appendChild(histSec);
    }catch(e){c.appendChild(UI.el('div','panel-empty','Tools info unavailable'))}
  }

  async function renderGateway(){
    const c=cont();c.innerHTML='<h3>Gateway Platforms</h3>';
    try{
      const platforms=await Api.listPlatforms();
      if(!platforms||!platforms.length){
        c.appendChild(UI.el('div','panel-empty','No platforms configured.\nEnable feishu/wechat/email in config.yaml'));
        return;
      }
      platforms.forEach(async function(p){
        const sec=UI.el('div','panel-section');
        sec.innerHTML='<h3>'+UI.esc(p.name||p)+'</h3>';
        c.appendChild(sec);
        try{
          const sessions=await Api.getPlatform(p.name||p);
          if(sessions&&sessions.sessions){
            sessions.sessions.forEach(function(s){
              const item=UI.el('div','panel-item');
              item.innerHTML='<span class="pi-name">'+UI.esc(s.id||'').slice(0,12)+'...</span><span style="font-size:11px;color:var(--text3)"> '+UI.esc(s.title||'')+'</span>';
              sec.appendChild(item);
            });
          }
        }catch(e){sec.appendChild(UI.el('div','panel-empty','Sessions unavailable'))}
      });
    }catch(e){c.appendChild(UI.el('div','panel-empty','Gateway info unavailable'))}
  }

  function auditRecordSummary(record){
    const r=record||{};
    const raw=r.record||{};
    return r.summary || raw.summary || raw.operation || raw.action || raw.tool || raw.id || r.id || 'Audit record';
  }

  function auditRecordTime(record){
    const value=(record&&record.timestamp)||(record&&record.record&&record.record.timestamp);
    if(!value)return '';
    try{
      return new Date(value).toLocaleString();
    }catch(e){
      return String(value);
    }
  }

  async function renderAudit(source){
    const c=cont();c.innerHTML='<h3>Enterprise Audit</h3>';
    const controls=UI.el('div','panel-section audit-controls');
    const select=UI.el('select');
    select.id='audit-source';
    select.innerHTML='<option value="all">All Sources</option><option value="memory">Memory</option><option value="approval">Approval</option>';
    select.value=source||'all';
    const refresh=UI.el('button','btn-secondary');
    refresh.textContent='Refresh';
    refresh.onclick=function(){renderAudit(select.value)};
    select.onchange=function(){renderAudit(this.value)};
    controls.appendChild(select);
    controls.appendChild(refresh);
    c.appendChild(controls);

    const summary=UI.el('div','panel-section audit-summary');
    summary.textContent='Loading audit records...';
    c.appendChild(summary);

    const list=UI.el('div','panel-section audit-records');
    c.appendChild(list);
    try{
      const data=await Api.exportAudit({source:select.value,limit:50,offset:0});
      const totals=data.totals||{};
      summary.innerHTML='<div class="audit-metrics">'
        +'<span><b>'+UI.esc(data.total??0)+'</b><small>shown</small></span>'
        +'<span><b>'+UI.esc(totals.memory??0)+'</b><small>memory</small></span>'
        +'<span><b>'+UI.esc(totals.approval??0)+'</b><small>approval</small></span>'
        +'</div>';

      const records=data.records||[];
      if(!records.length){
        list.appendChild(UI.el('div','panel-empty','No audit records'));
        return;
      }
      records.forEach(function(record){
        const item=UI.el('div','panel-item audit-record');
        const sourceLabel=UI.el('span','audit-source');
        sourceLabel.textContent=record.source||select.value;
        const body=UI.el('span','pi-name audit-body');
        const title=UI.el('span','audit-title');
        title.textContent=auditRecordSummary(record);
        const meta=UI.el('span','audit-meta');
        meta.textContent=auditRecordTime(record);
        body.appendChild(title);
        body.appendChild(meta);
        item.appendChild(sourceLabel);
        item.appendChild(body);
        item.onclick=function(){UI.showToast(JSON.stringify(record.record||record,null,2))};
        list.appendChild(item);
      });
    }catch(e){
      summary.textContent='Audit unavailable';
      list.appendChild(UI.el('div','panel-empty','Error: '+e.message));
    }
  }

  async function renderMemorySpatial(){
    const c=cont();c.innerHTML='<h3>Memory Palace (Spatial)</h3>';
    c.appendChild(UI.el('div','panel-section','<p style="font-size:12px;color:var(--text2)">Wing (project) → Room (date) → Drawer (exact text)</p>'));
    const tree=UI.el('div','panel-section');
    tree.style.cssText='font-family:monospace;font-size:12px;line-height:1.6';
    tree.innerHTML='<b>WING: cowd</b><br>';
    try{
      const layers=await Api.listMemoryLayers();
      for(const l of (layers.layers||layers||[])){
        tree.innerHTML+='&nbsp;&nbsp;<b>ROOM:</b> '+UI.esc(l.name||'layer'+l.index)+'<br>';
        try{
          const data=await Api.getMemoryLayer(l.name||l);
          const entries=data.entries||data||[];
          if(entries.length){
            entries.slice(0,8).forEach(e=>{
              const title=UI.esc((e.title||e.content||'').substring(0,60));
              tree.innerHTML+='&nbsp;&nbsp;&nbsp;&nbsp;<span style="color:var(--text3)">DRAWER:</span> '+title+'<br>';
            });
            if(entries.length>8) tree.innerHTML+='&nbsp;&nbsp;&nbsp;&nbsp;<span style="color:var(--text3)">... +'+(entries.length-8)+' more</span><br>';
          }
        }catch(ex){tree.innerHTML+='&nbsp;&nbsp;&nbsp;&nbsp;<span style="color:var(--text3)">DRAWER:</span> unavailable<br>';}
      }
    }catch(e){tree.innerHTML+='&nbsp;&nbsp;unavailable'}
    c.appendChild(tree);
    c.appendChild(UI.el('div','panel-section','<button onclick="Panels.renderMemory()">← List View</button>'));
  }

  async function renderMemoryNetwork(){
    const c=cont();c.innerHTML='<h3>Knowledge Network</h3>';
    const graphSec=UI.el('div','panel-section');
    c.appendChild(graphSec);
    try{
      const triples=await Api.listTriples();
      const linkData=await Api.memoryLinks();
      const links=linkData.links||linkData||[];
      renderKnowledgeGraph(buildKnowledgeNetwork(triples,links),graphSec);
      const rows=UI.el('div','panel-section');
      rows.innerHTML='<h3>Relations</h3>';
      (triples||[]).slice(0,10).forEach(function(t){
        rows.appendChild(UI.el('div','panel-item',(UI.esc(t.subject||t.s||'')+' → '+UI.esc(t.predicate||t.p||'')+' → '+UI.esc(t.object||t.o||''))));
      });
      links.slice(0,10).forEach(function(l){
        rows.appendChild(UI.el('div','panel-item',(UI.esc(String(l.from||'').slice(0,8))+' → '+UI.esc(l.kind||'Link')+' → '+UI.esc(String(l.to||'').slice(0,8)))));
      });
      c.appendChild(rows);
    }catch(e){
      graphSec.appendChild(UI.el('div','panel-empty','Network unavailable'));
    }
    c.appendChild(UI.el('div','panel-section','<button onclick="Panels.renderMemory()">← Memory</button>'));
  }

  async function renderProgress(){
    const c=cont();c.innerHTML='<h3>Workflow Progress</h3>';
    try{
      const p=await Api.getProgress();
      const pct=Math.round((p.completed/p.total)*100)||0;
      const bar='█'.repeat(Math.floor(pct/5))+'░'.repeat(20-Math.floor(pct/5));
      c.appendChild(UI.el('div','panel-section',
        `<div style="font-family:monospace;font-size:14px">${bar} ${pct}%</div>
         <div style="font-size:12px;color:var(--text2);margin-top:4px">${p.current_phase||'idle'}</div>`));
    }catch(e){c.appendChild(UI.el('div','panel-section','Progress unavailable'))}
  }

  return{renderMemory,renderContext,renderMemoryLayer,renderMemorySymbolResults,showMemoryEntryForm,renderMemorySpatial,renderMemoryNetwork,renderProgress,renderSkills,renderCrons,renderSettings,renderAgents,renderTools,renderGateway,renderAudit,renderCCConfig,renderCCProviders,renderCCApproval,renderCCHistory,renderCCUsage};
})();
