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
    const evidence=(item.evidence||[]).slice(0,2);
    if(evidence.length){
      const refs=UI.el('em','context-evidence');
      refs.appendChild(document.createTextNode('refs '));
      evidence.forEach(function(ref,index){
        if(index)refs.appendChild(document.createTextNode(' · '));
        const btn=UI.el('button','context-evidence-ref');
        btn.type='button';
        btn.textContent=ref;
        btn.onclick=async function(){
          try{
            const resolved=await Api.resolveEvidence(ref,{session_id:Api.sid});
            let detail=body.querySelector('.context-evidence-detail');
            if(!detail){
              detail=UI.el('pre','context-evidence-detail');
              body.appendChild(detail);
            }
            detail.textContent=JSON.stringify(resolved,null,2).slice(0,1600);
          }catch(e){UI.showToast(e.message,'error')}
        };
        refs.appendChild(btn);
      });
      body.appendChild(refs);
    }
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

  function renderLeanProbe(probe,policy){
    const sec=UI.el('div','context-lean-probe');
    if(!probe)return sec;
    sec.innerHTML='<h4>Runtime Probe</h4>';
    const metrics=UI.el('div','memory-metrics');
    metrics.appendChild(renderMemoryMetric('level',probe.pressure_level||'Nominal','pressure'));
    metrics.appendChild(renderMemoryMetric('fallback',probe.degradation_path||'None','path'));
    metrics.appendChild(renderMemoryMetric('selected',probe.selected_count||0,'items'));
    metrics.appendChild(renderMemoryMetric('omitted',probe.omitted_count||0,'items'));
    metrics.appendChild(renderMemoryMetric('stable',shortHash(probe.stable_head_hash),'cache'));
    metrics.appendChild(renderMemoryMetric('tail',shortHash(probe.dynamic_tail_hash),'cache'));
    if(policy&&policy.action){
      metrics.appendChild(renderMemoryMetric('policy',policy.action,'action'));
    }
    sec.appendChild(metrics);
    if(policy&&policy.reason){
      const reason=UI.el('small','runtime-policy-reason');
      reason.textContent=policy.reason;
      sec.appendChild(reason);
    }
    return sec;
  }

  function renderModeCoverage(coverage,stability){
    const sec=UI.el('div','context-mode-coverage');
    if(!coverage)return sec;
    sec.innerHTML='<h4>Mode Coverage</h4>';
    const entries=coverage.entries||[];
    const metrics=UI.el('div','memory-metrics');
    metrics.appendChild(renderMemoryMetric('profiles',entries.length+'/'+((coverage.required_profiles||[]).length||entries.length),'covered'));
    metrics.appendChild(renderMemoryMetric('stable head',coverage.all_stable_heads_reusable?'reused':'changed','cache'));
    if(stability){
      metrics.appendChild(renderMemoryMetric('cache',stability.prompt_cache_friendly?'friendly':'break','kv'));
    }
    sec.appendChild(metrics);
    if(stability&&stability.reason){
      const reason=UI.el('small','runtime-policy-reason');
      reason.textContent=stability.reason;
      sec.appendChild(reason);
    }
    const list=UI.el('div','context-mode-list');
    entries.slice(0,8).forEach(function(entry){
      const row=UI.el('div','context-mode-row');
      row.innerHTML='<b>'+UI.esc(entry.profile||'profile')+'</b><small>'+UI.esc([entry.mode||'mode',entry.stable_head_reusable?'stable':'changed','pressure '+fmtPressure(entry.pressure_bp)].join(' · '))+'</small>';
      list.appendChild(row);
    });
    sec.appendChild(list);
    return sec;
  }

  function renderContextHistoryItem(item,onSelect){
    const row=UI.el('button','context-history-item');
    const envelope=item.envelope||{};
    const diagnostics=envelope.diagnostics||{};
    const profile=envelope.profile||item.profile||'Context';
    const intent=envelope.intent||item.intent||'';
    const pressure=diagnostics.pressure_bp??item.pressure_bp;
    const stamp=item.created_at_ms?new Date(item.created_at_ms).toLocaleTimeString():'n/a';
    row.type='button';
    row.innerHTML='<b>'+UI.esc(profile)+'</b><small>'+UI.esc([item.envelope_id||envelope.id||'no-id',item.run_id||'no-run','seq '+(item.sequence??'n/a'),stamp].join(' · '))+'</small><em>'+UI.esc(contextTextPreview(intent))+'</em><span>'+UI.esc(fmtPressure(pressure))+'</span>';
    row.onclick=function(){if(onSelect)onSelect(item)};
    return row;
  }

  function renderContextHistoryDetail(item){
    const detail=UI.el('div','context-history-detail');
    const envelope=(item&&item.envelope)||{};
    const diagnostics=envelope.diagnostics||{};
    detail.innerHTML='<h4>'+UI.esc(envelope.profile||'Context')+'</h4><small>'+UI.esc([item.envelope_id||envelope.id||'no-id',item.run_id||'no-run','pressure '+fmtPressure(diagnostics.pressure_bp),'selected '+((envelope.selected||[]).length),'omitted '+((envelope.omitted||[]).length)].join(' · '))+'</small>';
    const selected=(envelope.selected||[]).slice(0,3);
    if(selected.length){
      selected.forEach(function(ctxItem){detail.appendChild(renderContextItem(ctxItem))});
    }else{
      detail.appendChild(UI.el('div','panel-empty','No selected context in this envelope'));
    }
    return detail;
  }

  function contextHistoryRows(timeline){
    if(timeline&&timeline.summaries&&timeline.summaries.length)return timeline.summaries;
    return (timeline&&timeline.envelopes)||[];
  }

  function renderRuntimeRunItem(item,onContextSelect){
    const row=UI.el('div','runtime-run-item');
    const run=(item&&item.run)||{};
    const stamp=item.created_at_ms?new Date(item.created_at_ms).toLocaleTimeString():'n/a';
    row.innerHTML='<b>'+UI.esc(run.status||run.phase||'run')+'</b><small>'+UI.esc([run.profile||'MainTurn',run.run_id||'no-run-id','seq '+(item.sequence??'n/a'),stamp].join(' · '))+'</small><em>'+UI.esc(contextTextPreview(run.intent_preview||run.error||run.context_envelope_id||''))+'</em>';
    if(run.context_envelope_id&&onContextSelect){
      const link=UI.el('button','runtime-context-link');
      link.type='button';
      link.textContent='context '+run.context_envelope_id;
      link.onclick=function(){onContextSelect({envelope_id:run.context_envelope_id,run_id:run.run_id,sequence:item.sequence,created_at_ms:item.created_at_ms})};
      row.appendChild(link);
    }
    return row;
  }

  function renderRuntimeTimelineItem(item,onContextSelect){
    const row=UI.el('div','runtime-run-item runtime-timeline-item');
    const stamp=item.created_at_ms?new Date(item.created_at_ms).toLocaleTimeString():'n/a';
    const refsList=(item.refs||[]).filter(function(ref){return ref&&ref.id});
    const refs=refsList.map(function(ref){return (ref.type||ref.ref_type||'ref')+':'+(ref.id||'')}).filter(Boolean).slice(0,3).join(' · ');
    const payload=item.payload||{};
    const score=payload.scorecard||{};
    const complexity=(payload.complexity)||{};
    const policyText=item.kind==='runtime.policy.decided'
      ? 'level '+(complexity.level||'n/a')+' · score '+(complexity.score??'n/a')+' · agent '+(payload.agent_mode||'n/a')+' · review '+(payload.requires_review?'yes':'no')
      : '';
    const scoreText=payload.board_id
      ? 'board '+payload.board_id+' · complete '+fmtPct(score.completion_rate)+' · conflicts '+fmtNumber(score.conflict_count)+' · candidates '+((payload.maintenance_candidates||[]).length)
      : '';
    row.innerHTML='<b>'+UI.esc(item.kind||'event')+'</b><small>'+UI.esc([item.scope||'runtime','seq '+(item.sequence??'n/a'),stamp].join(' · '))+'</small><em>'+UI.esc(contextTextPreview(policyText||refs||scoreText||payload.summary||payload.error||payload.intent_preview||''))+'</em>';
    const contextRefs=refsList.filter(function(ref){return (ref.type||ref.ref_type)==='context_envelope'});
    if(contextRefs.length&&onContextSelect){
      const refRow=UI.el('div','runtime-ref-actions');
      contextRefs.slice(0,2).forEach(function(ref){
        const btn=UI.el('button','runtime-context-link');
        btn.type='button';
        btn.textContent='context '+ref.id;
        btn.onclick=function(){onContextSelect(ref.id)};
        refRow.appendChild(btn);
      });
      row.appendChild(refRow);
    }
    return row;
  }

  function renderDispatchTarget(target){
    const row=UI.el('div','panel-item audit-record dispatch-target '+(target&&target.ready?'ready':'blocked'));
    const source=UI.el('span','audit-source');
    source.textContent=target&&target.ready?'ready':'blocked';
    const body=UI.el('span','pi-name audit-body');
    const title=UI.el('span','audit-title');
    const outbound=(target&&target.outbound_message)||{};
    const targetName=[target&&target.platform,target&&target.operation].filter(Boolean).join(' · ');
    title.textContent='Dispatch Target'+(targetName?' · '+targetName:'');
    const meta=UI.el('span','audit-meta');
    const parts=[];
    if(target&&target.session_key)parts.push('session '+target.session_key);
    if(outbound.text)parts.push('payload '+String(outbound.text).slice(0,48));
    const blockers=(target&&target.blockers)||[];
    if(blockers.length)parts.push(blockers.slice(0,2).join(' · '));
    meta.textContent=parts.join(' · ')||'no target plan';
    body.appendChild(title);
    body.appendChild(meta);
    row.appendChild(source);
    row.appendChild(body);
    return row;
  }

  function renderDispatchOutcome(outcome){
    const status=(outcome&&outcome.status)||'unknown';
    const row=UI.el('div','panel-item audit-record dispatch-outcome '+status);
    const source=UI.el('span','audit-source');
    source.textContent=status;
    const body=UI.el('span','pi-name audit-body');
    const title=UI.el('span','audit-title');
    const targetName=[outcome&&outcome.platform,outcome&&outcome.operation].filter(Boolean).join(' · ');
    title.textContent='Dispatch Outcome'+(targetName?' · '+targetName:'');
    const meta=UI.el('span','audit-meta');
    const parts=[];
    if(outcome&&outcome.session_key)parts.push('session '+outcome.session_key);
    if(outcome&&outcome.provider_message_id)parts.push('provider '+outcome.provider_message_id);
    if(outcome&&outcome.error)parts.push(String(outcome.error).slice(0,72));
    meta.textContent=parts.join(' · ')||'no delivery detail';
    body.appendChild(title);
    body.appendChild(meta);
    row.appendChild(source);
    row.appendChild(body);
    return row;
  }

  function crossPlanePayloadRef(operation,value){
    const raw=String(value||'').trim();
    if(operation==='send_text'){
      return raw.startsWith('text://')||raw.startsWith('text:')?raw:'text://'+raw;
    }
    if(operation==='send_image'){
      if(raw.startsWith('image://')||raw.startsWith('http://')||raw.startsWith('https://')||raw.startsWith('workspace://'))return raw.startsWith('image://')?raw:'image://'+raw;
      return 'image://'+raw;
    }
    if(operation==='send_file'){
      if(raw.startsWith('file://')||raw.startsWith('workspace://'))return raw;
      return 'file://'+raw;
    }
    return raw;
  }

  function renderCrossPlaneComposer(){
    const box=UI.el('div','cross-plane-composer');
    box.innerHTML='<h3>Action Composer</h3>';
    const form=UI.el('div','panel-form cross-plane-form');

    const operation=UI.el('select');
    operation.setAttribute('aria-label','Operation');
    [
      ['send_text','Send text'],
      ['send_image','Send image'],
      ['send_file','Send file'],
    ].forEach(function(pair){
      const opt=UI.el('option');
      opt.value=pair[0];
      opt.textContent=pair[1];
      operation.appendChild(opt);
    });

    const principal=UI.el('input');
    principal.placeholder='user:demo';
    principal.value='user:demo';
    principal.setAttribute('aria-label','Principal');

    const target=UI.el('input');
    target.placeholder='channel://feishu/chat/demo';
    target.value='channel://feishu/chat/demo';
    target.setAttribute('aria-label','Target channel ref');

    const payload=UI.el('textarea');
    payload.placeholder='Message text, image URL, or workspace file path';
    payload.value='hello from cowd';
    payload.setAttribute('aria-label','Payload');

    const mode=UI.el('select');
    mode.setAttribute('aria-label','Execution mode');
    [['dry_run','Dry run'],['commit','Commit']].forEach(function(pair){
      const opt=UI.el('option');
      opt.value=pair[0];
      opt.textContent=pair[1];
      mode.appendChild(opt);
    });

    const row=UI.el('div','cross-plane-form-grid');
    row.appendChild(operation);
    row.appendChild(mode);
    form.appendChild(row);
    form.appendChild(principal);
    form.appendChild(target);
    form.appendChild(payload);

    const actions=UI.el('div','cross-plane-actions');
    const preflightBtn=UI.el('button','btn-secondary');
    preflightBtn.textContent='Preflight action';
    const executeBtn=UI.el('button','btn-secondary');
    executeBtn.textContent='Execute action';
    actions.appendChild(preflightBtn);
    actions.appendChild(executeBtn);
    form.appendChild(actions);

    const resultBox=UI.el('div','dispatch-target-preview');

    function actionBody(){
      const op=operation.value;
      return {
        actor_principal:principal.value.trim()||'user:demo',
        source_channel:'local:webui',
        session_id:'webui-composer',
        requested_capability:'channel.feishu.'+op,
        provider_account:'feishu-main',
        target_ref:target.value.trim(),
        resource_ref:crossPlanePayloadRef(op,payload.value),
        risk:op==='send_text'?'low':'high',
        data_classification:'internal',
        identity_trust:'verified'
      };
    }

    function setBusy(busy){
      preflightBtn.disabled=busy;
      executeBtn.disabled=busy;
    }

    function renderResult(title,result){
      resultBox.innerHTML='<h3>'+UI.esc(title)+'</h3>';
      if(result.dispatch_target)resultBox.appendChild(renderDispatchTarget(result.dispatch_target));
      if(result.dispatch_outcome)resultBox.appendChild(renderDispatchOutcome(result.dispatch_outcome));
      if((result.blockers||[]).length){
        const blockers=UI.el('div','panel-empty');
        blockers.textContent='Blocked '+result.blockers.slice(0,3).join(' · ');
        resultBox.appendChild(blockers);
      }
    }

    operation.onchange=function(){
      if(operation.value==='send_text')payload.value='hello from cowd';
      else if(operation.value==='send_image')payload.value='https://example.test/panel.png';
      else payload.value='workspace://file/reports/panel.txt';
    };

    preflightBtn.onclick=async function(){
      setBusy(true);
      try{
        const result=await Api.preflightCrossPlaneAction(actionBody());
        renderResult('Preflight Result',result);
        UI.showToast((result.dispatch_target&&result.dispatch_target.ready)?'Dispatch target ready':'Dispatch target blocked');
      }catch(e){
        UI.showToast('Preflight failed: '+e.message,'error');
      }finally{
        setBusy(false);
      }
    };

    executeBtn.onclick=async function(){
      setBusy(true);
      try{
        const result=await Api.executeCrossPlaneAction({
          mode:mode.value,
          idempotency_key:'webui-'+Date.now(),
          action:actionBody()
        });
        renderResult('Execution Result',result);
        UI.showToast(result.dispatched?'Dispatch sent':'Dispatch '+(result.dispatch_status||result.status));
      }catch(e){
        UI.showToast('Execute failed: '+e.message,'error');
      }finally{
        setBusy(false);
      }
    };

    box.appendChild(form);
    box.appendChild(resultBox);
    return box;
  }

  function latestPolicyDecision(events){
    const policyEvents=(events||[]).filter(function(item){return item&&item.kind==='runtime.policy.decided'});
    const latest=policyEvents[policyEvents.length-1]||null;
    const payload=(latest&&latest.payload)||{};
    const complexity=payload.complexity||{};
    return {
      count: policyEvents.length,
      level: complexity.level||'n/a',
      score: complexity.score??'n/a',
      agent: payload.agent_mode||'n/a',
      review: payload.requires_review?'yes':'no',
      signals: (complexity.signals||[]).length,
    };
  }

  function summarizeWorkGraphs(events,serverSummary){
    if(serverSummary&&typeof serverSummary==='object'){
      const latest=serverSummary.latest||{};
      return {
        count: fmtNumber(serverSummary.count),
        latest: serverSummary.count?latest:null,
        graph: {graph_id: latest.graph_id, status: latest.status},
        score: {
          completion_rate: latest.completion_rate,
          synthesis_lift: latest.synthesis_lift,
          complementarity_score: latest.complementarity_score,
          value_verdict: latest.value_verdict,
        },
        candidates: new Array(fmtNumber(serverSummary.memory_candidates)),
        boardId: latest.board_id||'n/a',
        graphId: latest.graph_id||'n/a',
        status: latest.status||'n/a',
        agentTasks: fmtNumber(serverSummary.agent_tasks),
        conflicts: fmtNumber(serverSummary.conflicts),
      };
    }
    const graphEvents=(events||[]).filter(function(item){
      return item && (item.kind==='agent.workgraph.reviewed' || item.kind==='agent.workgraph.planned');
    });
    const latest=graphEvents[graphEvents.length-1]||null;
    const payload=(latest&&latest.payload)||{};
    const graph=payload.graph||{};
    const score=payload.scorecard||{};
    const verdict=payload.value_verdict||score.value_verdict||{};
    const candidates=payload.maintenance_candidates||[];
    return {
      count: graphEvents.length,
      latest,
      graph,
      score,
      verdict,
      candidates,
      boardId: payload.board_id||graph.board_id||'n/a',
      graphId: graph.graph_id||((latest&&latest.refs||[]).find(function(ref){return (ref.type||ref.ref_type)==='workgraph'})||{}).id||'n/a',
      status: graph.status||(latest&&latest.status)||'n/a',
      agentTasks: (graph.nodes||[]).filter(function(node){return node.kind==='AgentTask'||node.kind==='agent_task'}).length,
      conflicts: fmtNumber(score.conflict_count),
    };
  }

  function renderWorkGraphSummary(events,serverSummary){
    const summary=summarizeWorkGraphs(events,serverSummary);
    const sec=UI.el('div','runtime-workgraph-summary');
    sec.innerHTML='<h4>Agent WorkGraph</h4>';
    const metrics=UI.el('div','memory-metrics');
    metrics.appendChild(renderMemoryMetric('graphs',summary.count,'events'));
    metrics.appendChild(renderMemoryMetric('status',summary.status,'latest'));
    metrics.appendChild(renderMemoryMetric('agents',summary.agentTasks,'tasks'));
    metrics.appendChild(renderMemoryMetric('complete',fmtPct(summary.score.completion_rate),'score'));
    metrics.appendChild(renderMemoryMetric('value',fmtNumber((summary.verdict||summary.score.value_verdict||{}).value_score),'lift'));
    metrics.appendChild(renderMemoryMetric('conflicts',summary.conflicts,'review'));
    metrics.appendChild(renderMemoryMetric('candidates',summary.candidates.length,'memory'));
    sec.appendChild(metrics);
    if(!summary.latest){
      sec.appendChild(UI.el('div','panel-empty','No agent workgraph events'));
      return sec;
    }
    const detail=UI.el('div','runtime-workgraph-detail');
    const verdict=summary.verdict||summary.score.value_verdict||{};
    detail.innerHTML='<b>'+UI.esc(summary.graphId)+'</b><small>'+UI.esc(['board '+summary.boardId,'lift '+fmtPct(summary.score.synthesis_lift),'complement '+fmtPct(summary.score.complementarity_score),'positive '+(verdict.positive_lift?'yes':'no')].join(' · '))+'</small>';
    sec.appendChild(detail);
    return sec;
  }

  function renderRuntimeHealthSummary(summary){
    const sec=UI.el('div','runtime-workgraph-summary runtime-health-summary');
    sec.innerHTML='<h4>Runtime Health</h4>';
    const metrics=UI.el('div','memory-metrics');
    const health=summary||{};
    metrics.appendChild(renderMemoryMetric('status',health.status||'unknown','health'));
    metrics.appendChild(renderMemoryMetric('score',fmtNumber(health.score),'quality'));
    metrics.appendChild(renderMemoryMetric('failed',fmtNumber(health.failed_events),'events'));
    metrics.appendChild(renderMemoryMetric('degraded',fmtNumber(health.degraded_events),'events'));
    metrics.appendChild(renderMemoryMetric('open',fmtNumber(health.open_tasks),'tasks'));
    metrics.appendChild(renderMemoryMetric('agent lift',health.positive_agent_lift?'yes':'no','value'));
    sec.appendChild(metrics);
    const reasons=health.reasons||[];
    if(reasons.length){
      const detail=UI.el('div','runtime-workgraph-detail');
      detail.innerHTML='<b>'+UI.esc(reasons[0])+'</b><small>'+UI.esc('latest value '+fmtNumber(health.latest_value_score)+' · events '+fmtNumber(health.event_count))+'</small>';
      sec.appendChild(detail);
    }
    return sec;
  }

  function renderValueLoopSummary(summary){
    const loop=summary||{};
    const sec=UI.el('div','runtime-workgraph-summary runtime-value-loop-summary');
    sec.innerHTML='<h4>Value Loop</h4>';
    const metrics=UI.el('div','memory-metrics');
    metrics.appendChild(renderMemoryMetric('status',loop.status||'unknown','loop'));
    metrics.appendChild(renderMemoryMetric('score',fmtNumber(loop.score),'closure'));
    metrics.appendChild(renderMemoryMetric('covered',fmtNumber(loop.required_observed)+'/'+fmtNumber(loop.required_total),'stages'));
    metrics.appendChild(renderMemoryMetric('missing',fmtNumber(loop.missing_required_count),'required'));
    metrics.appendChild(renderMemoryMetric('failed',fmtNumber(loop.failed_events),'events'));
    metrics.appendChild(renderMemoryMetric('open',fmtNumber(loop.open_tasks),'tasks'));
    sec.appendChild(metrics);
    const stages=loop.stages||[];
    if(stages.length){
      const grid=UI.el('div','runtime-value-loop-stages');
      stages.forEach(function(stage){
        const item=UI.el('span','runtime-value-loop-stage runtime-value-loop-stage-'+(stage.status||'unknown'));
        item.textContent=(stage.label||stage.id||'stage')+' · '+(stage.status||'unknown');
        item.title=[stage.id,stage.latest_kind||'',stage.latest_sequence!==undefined?'seq '+stage.latest_sequence:''].filter(Boolean).join(' · ');
        grid.appendChild(item);
      });
      sec.appendChild(grid);
    }
    const reasons=loop.reasons||loop.next_actions||[];
    if(reasons.length){
      const detail=UI.el('div','runtime-workgraph-detail');
      detail.innerHTML='<b>'+UI.esc(reasons[0])+'</b><small>'+UI.esc((loop.next_actions||[]).slice(0,2).join(' · '))+'</small>';
      sec.appendChild(detail);
    }
    return sec;
  }

  function renderAgentValueSummary(summary){
    const agent=summary||{};
    const latest=agent.latest||{};
    const policy=agent.policy||{};
    const sec=UI.el('div','runtime-workgraph-summary runtime-agent-value-summary');
    sec.innerHTML='<h4>Agent Value</h4>';
    const metrics=UI.el('div','memory-metrics');
    metrics.appendChild(renderMemoryMetric('status',agent.status||'unknown','agent'));
    metrics.appendChild(renderMemoryMetric('decision',agent.recommendation||'n/a','next'));
    metrics.appendChild(renderMemoryMetric('score',fmtNumber(latest.value_score),'value'));
    metrics.appendChild(renderMemoryMetric('threshold',fmtNumber(policy.min_collaboration_score),'policy'));
    metrics.appendChild(renderMemoryMetric('lift',latest.positive_lift?'yes':'no','proven'));
    metrics.appendChild(renderMemoryMetric('conflicts',fmtNumber(latest.conflict_count),'review'));
    sec.appendChild(metrics);
    const reasons=agent.reasons||[];
    if(reasons.length){
      const detail=UI.el('div','runtime-workgraph-detail');
      detail.innerHTML='<b>'+UI.esc(reasons[0])+'</b><small>'+UI.esc(['agents '+fmtNumber(latest.agent_tasks),'synthesis '+fmtPct(latest.synthesis_lift),'complement '+fmtPct(latest.complementarity_score)].join(' · '))+'</small>';
      sec.appendChild(detail);
    }
    return sec;
  }

  function renderRuntimeControlPlane(plane){
    const sec=UI.el('div','runtime-control-plane');
    sec.innerHTML='<h4>Control Plane</h4>';
    if(!plane){
      sec.appendChild(UI.el('div','panel-empty','Control plane unavailable'));
      return sec;
    }
    const components=plane.components||{};
    const session=components.session||{};
    const memory=components.memory||{};
    const context=components.context||{};
    const agent=components.agent||{};
    const task=components.task||{};
    const permissions=components.permissions||{};
    const provider=components.provider||{};
    const channels=components.channels||{};
    const config=plane.config||{};
    const diagnostics=plane.diagnostics||{};
    const readiness=plane.readiness||{};
    const head=UI.el('div','runtime-control-plane-head');
    head.innerHTML='<span class="'+memoryHealthClass({status:plane.status,degraded:plane.degraded})+'">'+UI.esc(plane.status||'unknown')+'</span><small>'+UI.esc([config.scenario,config.source,plane.profile_id].filter(Boolean).join(' · '))+'</small>';
    const reloadBtn=UI.el('button','btn-secondary btn-xs','Reload providers');
    reloadBtn.type='button';
    reloadBtn.onclick=async function(){
      reloadBtn.disabled=true;
      try{
        const result=await Api.runtimeReloadProviders();
        UI.showToast('Providers '+(result.status||'reloaded'),'success');
        sec.dataset.providerReloadStatus=result.status||'unknown';
        sec.dataset.providerReloadApplied=result.applied?'true':'false';
        await renderRuntimeConsole();
      }catch(err){
        UI.showToast(err.message,'error');
      }finally{
        reloadBtn.disabled=false;
      }
    };
    head.appendChild(reloadBtn);
    sec.appendChild(head);
    const metrics=UI.el('div','memory-metrics');
    metrics.appendChild(renderMemoryMetric('store',session.source_of_truth||'n/a','session'));
    metrics.appendChild(renderMemoryMetric('active',session.active_count??0,'sessions'));
    metrics.appendChild(renderMemoryMetric('memory',memory.status||'n/a',memory.search_mode||'mode'));
    metrics.appendChild(renderMemoryMetric('history',context.durable_history?'on':'off','context'));
    metrics.appendChild(renderMemoryMetric('agents',agent.max_parallel_agents??0,agent.status||'policy'));
    metrics.appendChild(renderMemoryMetric('tasks',task.open??0,'open'));
    metrics.appendChild(renderMemoryMetric('auth',permissions.auth_required?'on':'off','permission'));
    metrics.appendChild(renderMemoryMetric('gate',permissions.approval_gate?'on':'off','approval'));
    metrics.appendChild(renderMemoryMetric('provider',provider.status||'n/a','model'));
    metrics.appendChild(renderMemoryMetric('models',provider.model_count??diagnostics.provider_model_count??0,'provider'));
    metrics.appendChild(renderMemoryMetric('route',provider.configured_model_resolved?'ok':'miss','model'));
    metrics.appendChild(renderMemoryMetric('channels',(channels.adapters||[]).length,'adapters'));
    metrics.appendChild(renderMemoryMetric('stored',diagnostics.stored_sessions??'n/a','sessions'));
    metrics.appendChild(renderMemoryMetric('latency',(diagnostics.elapsed_ms??0)+'ms','control'));
    metrics.appendChild(renderMemoryMetric('perf',diagnostics.performance_status||'n/a','control'));
    metrics.appendChild(renderMemoryMetric('ready',(readiness.score??diagnostics.readiness_score??0)+'%','runtime'));
    metrics.appendChild(renderMemoryMetric('blocked',readiness.required_blocked??diagnostics.blocked_required_count??0,'required'));
    metrics.appendChild(renderMemoryMetric('components',diagnostics.component_count??0,'diag'));
    metrics.appendChild(renderMemoryMetric('caps',diagnostics.capability_count??(plane.capabilities||[]).length,'diag'));
    sec.appendChild(metrics);
    if((plane.degraded_reasons||[]).length){
      const reasons=UI.el('div','runtime-control-plane-reasons');
      reasons.textContent='degraded: '+plane.degraded_reasons.slice(0,3).join(' · ');
      sec.appendChild(reasons);
    }
    const blocked=(readiness.blocked||[]).slice(0,3);
    if(blocked.length){
      const blockedRow=UI.el('div','runtime-control-plane-reasons');
      blockedRow.textContent='blocked: '+blocked.map(function(check){return check.id||check.label||'required';}).join(' · ');
      sec.appendChild(blockedRow);
    }
    const next=(plane.next_actions||[]).slice(0,3);
    if(next.length){
      const nextRow=UI.el('div','runtime-control-plane-reasons');
      nextRow.textContent='next: '+next.join(' · ');
      sec.appendChild(nextRow);
    }
    const caps=(plane.capabilities||[]).slice(0,5);
    if(caps.length){
      const capRow=UI.el('div','runtime-control-plane-caps');
      caps.forEach(function(cap){capRow.appendChild(UI.el('span','memory-chip',UI.esc(cap)))});
      sec.appendChild(capRow);
    }
    return sec;
  }

  async function renderContext(){
    const c=cont();c.innerHTML='';
    const hdr=UI.el('div','panel-section context-header');
    hdr.innerHTML='<h3>Context Runtime</h3>';
    const controls=UI.el('div','context-controls');
    const input=UI.el('input');
    input.placeholder='Inspect intent...';
    const profile=UI.el('select');
    profile.innerHTML='<option value="MainTurn">Main</option><option value="SoloGoal">Solo</option><option value="YoloGoal">Yolo</option><option value="Review">Review</option><option value="Resume">Resume</option><option value="SubAgent">SubAgent</option><option value="Collaboration">Collab</option><option value="Cron">Cron</option>';
    const refresh=UI.el('button','btn-secondary btn-sm');
    refresh.textContent='Refresh';
    controls.appendChild(input);
    controls.appendChild(profile);
    controls.appendChild(refresh);
    hdr.appendChild(controls);
    c.appendChild(hdr);

    const mount=UI.el('div');
    c.appendChild(mount);
    async function load(){
      mount.innerHTML='';
      const opts={q:input.value||'',profile:profile.value};
      if(Api.sid)opts.session_id=Api.sid;
      try{
        const response=await Api.currentContext(opts);
        const envelope=(response&&response.envelope)||{};
        const leanProbe=(response&&response.lean_probe)||null;
        const policyDecision=(response&&response.policy_decision)||null;
        const modeCoverage=(response&&response.mode_coverage)||null;
        const cacheStability=(response&&response.cache_stability)||null;
        const diagnostics=envelope.diagnostics||{};
        const budget=envelope.budget||{};
        const assembled=envelope.assembled||{};
        let recommendationStats={};
        if(Api.sid){
          try{
            const stats=await Api.contextRecommendationStats(Api.sid,{limit:200});
            (stats.recommendations||[]).forEach(function(item){
              recommendationStats[item.recommendation]=item;
            });
          }catch(statsError){}
        }

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
        overview.appendChild(renderLeanProbe(leanProbe,policyDecision));
        overview.appendChild(renderModeCoverage(modeCoverage,cacheStability));
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
            const stat=recommendationStats[text];
            if(stat&&stat.count){
              const count=UI.el('span','context-rec-count');
              const actions=stat.actions||{};
              const ack=actions.acknowledged||0;
              const applied=actions.applied||0;
              count.textContent=applied?('applied '+applied):('ack '+ack);
              row.appendChild(count);
            }
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
        let showHistoryItem=null;
        if(!Api.sid){
          history.appendChild(UI.el('div','panel-empty','No active session'));
        }else{
          try{
            const detailMount=UI.el('div','context-history-detail-mount');
            const rowsMount=UI.el('div','context-history-rows');
            const controls=UI.el('div','context-history-controls');
            let nextSeq=null;
            let hasMore=false;
            let loaded=0;
            showHistoryItem=async function(item){
              detailMount.innerHTML='';
              let selected=item;
              const envelopeId=item.envelope_id||(item.envelope||{}).id;
              if(envelopeId){
                try{
                  const detail=await Api.contextEnvelope(envelopeId);
                  selected=detail.context||item;
                }catch(detailError){}
              }
              detailMount.appendChild(renderContextHistoryDetail(selected));
            };
            function appendHistoryRows(rows){
              rows.forEach(function(item){
                rowsMount.appendChild(renderContextHistoryItem(item,showHistoryItem));
              });
              loaded+=rows.length;
            }
            function renderHistoryControls(){
              controls.innerHTML='';
              if(!hasMore)return;
              const more=UI.el('button','context-history-more');
              more.type='button';
              more.textContent='Load more ('+loaded+')';
              more.onclick=async function(){
                more.disabled=true;
                more.textContent='Loading...';
                try{
                  const page=await Api.contextHistory(Api.sid,{from_seq:nextSeq,limit:8});
                  const rows=contextHistoryRows(page);
                  appendHistoryRows(rows);
                  nextSeq=page.next_seq;
                  hasMore=!!page.has_more&&rows.length>0;
                }catch(e){
                  controls.appendChild(UI.el('div','panel-empty','More context unavailable'));
                  hasMore=false;
                }
                renderHistoryControls();
              };
              controls.appendChild(more);
            }
            const timeline=await Api.contextHistory(Api.sid,{limit:8});
            const rows=contextHistoryRows(timeline);
            if(!rows.length)rowsMount.appendChild(UI.el('div','panel-empty','No persisted envelopes'));
            appendHistoryRows(rows);
            nextSeq=timeline.next_seq;
            hasMore=!!timeline.has_more&&rows.length>0;
            history.appendChild(rowsMount);
            renderHistoryControls();
            history.appendChild(controls);
            history.appendChild(detailMount);
            if(rows[0])showHistoryItem(rows[0]);
          }catch(historyError){
            history.appendChild(UI.el('div','panel-empty','Context timeline unavailable'));
          }
        }
        mount.appendChild(history);

        const runs=UI.el('div','panel-section runtime-runs');
        runs.innerHTML='<h3>Runtime Runs</h3>';
        if(!Api.sid){
          runs.appendChild(UI.el('div','panel-empty','No active session'));
        }else{
          try{
            const runTimeline=await Api.runtimeRuns(Api.sid,{limit:8});
            const rows=runTimeline.runs||[];
            if(!rows.length)runs.appendChild(UI.el('div','panel-empty','No runtime runs'));
            rows.slice(-8).reverse().forEach(function(item){
              runs.appendChild(renderRuntimeRunItem(item,showHistoryItem));
            });
          }catch(runError){
            runs.appendChild(UI.el('div','panel-empty','Runtime runs unavailable'));
          }
        }
        mount.appendChild(runs);
      }catch(e){
        mount.appendChild(UI.el('div','panel-empty','Context unavailable: '+e.message));
      }
    }
    refresh.onclick=load;
    input.onkeydown=function(e){if(e.key==='Enter')load()};
    profile.onchange=load;
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

  async function renderMemoryMaintenance(target){
    target.innerHTML='<h3>Memory Maintenance</h3>';
    const actions=UI.el('div','context-controls');
    const scan=UI.el('button');
    scan.textContent='Scan';
    scan.onclick=async()=>{
      try{
        await Api.scanMemoryMaintenance({max_candidates:50});
        await renderMemoryMaintenance(target);
      }catch(e){UI.showToast(e.message,'error')}
    };
    actions.appendChild(scan);
    target.appendChild(actions);
    try{
      const data=await Api.memoryMaintenance({status:'open',limit:8});
      const candidates=data.candidates||[];
      if(!candidates.length){
        target.appendChild(UI.el('div','panel-empty',data.degraded_reason||'No open maintenance candidates'));
        return;
      }
      candidates.forEach(candidate=>{
        const row=UI.el('div','panel-item memory-maintenance-item');
        const body=UI.el('div');
        body.innerHTML='<b>'+UI.esc(candidate.summary||candidate.kind||'candidate')+'</b><small>'+UI.esc([candidate.kind,candidate.status,(candidate.entry_ids||[]).length+' refs'].filter(Boolean).join(' · '))+'</small><em>'+UI.esc(candidate.reason||'')+'</em>';
        row.appendChild(body);
        const ack=UI.el('button');
        ack.textContent='Ack';
        ack.onclick=async(e)=>{
          e.stopPropagation();
          try{await Api.updateMemoryMaintenance(candidate.id,'acknowledged');await renderMemoryMaintenance(target)}catch(err){UI.showToast(err.message,'error')}
        };
        const dismiss=UI.el('button');
        dismiss.textContent='Dismiss';
        dismiss.onclick=async(e)=>{
          e.stopPropagation();
          try{await Api.updateMemoryMaintenance(candidate.id,'dismissed');await renderMemoryMaintenance(target)}catch(err){UI.showToast(err.message,'error')}
        };
        row.appendChild(ack);
        row.appendChild(dismiss);
        target.appendChild(row);
      });
    }catch(e){
      target.appendChild(UI.el('div','panel-empty','Maintenance unavailable'));
    }
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

    const maintenanceSec=UI.el('div','panel-section memory-maintenance');
    c.appendChild(maintenanceSec);
    await renderMemoryMaintenance(maintenanceSec);

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

  async function renderRuntimeConsole(){
    const c=cont();c.innerHTML='';
    const hdr=UI.el('div','panel-section context-header');
    hdr.innerHTML='<h3>Runtime Console</h3>';
    const controls=UI.el('div','context-controls');
    const input=UI.el('input');
    input.placeholder='Runtime intent...';
    const profile=UI.el('select');
    profile.innerHTML='<option value="MainTurn">Main</option><option value="SoloGoal">Solo</option><option value="YoloGoal">Yolo</option><option value="Review">Review</option><option value="Resume">Resume</option><option value="SubAgent">SubAgent</option><option value="Collaboration">Collab</option><option value="Cron">Cron</option>';
    const refresh=UI.el('button','btn-secondary btn-sm');
    refresh.textContent='Refresh';
    controls.appendChild(input);
    controls.appendChild(profile);
    controls.appendChild(refresh);
    hdr.appendChild(controls);
    c.appendChild(hdr);

    const grid=UI.el('div','runtime-console-grid');
    const summary=UI.el('div','panel-section runtime-console-summary');
    const runsSec=UI.el('div','panel-section runtime-runs');
    const timelineSec=UI.el('div','panel-section runtime-timeline');
    const contextSec=UI.el('div','panel-section context-list');
    const maintSec=UI.el('div','panel-section memory-maintenance');
    const runtimeDetailMount=UI.el('div','context-history-detail-mount runtime-context-detail-mount');
    grid.appendChild(summary);
    grid.appendChild(runsSec);
    grid.appendChild(timelineSec);
    grid.appendChild(contextSec);
    grid.appendChild(maintSec);
    c.appendChild(grid);

    async function load(){
      summary.innerHTML='<h3>Runtime State</h3>';
      runsSec.innerHTML='<h3>Runtime Runs</h3>';
      timelineSec.innerHTML='<h3>Runtime Timeline</h3>';
      contextSec.innerHTML='<h3>Active Context</h3>';
      maintSec.innerHTML='<h3>Memory Maintenance</h3>';
      runtimeDetailMount.innerHTML='';
      const opts={q:input.value||'',profile:profile.value};
      if(Api.sid)opts.session_id=Api.sid;

      async function showRuntimeContext(envelopeId){
        runtimeDetailMount.innerHTML='';
        if(!envelopeId)return;
        try{
          const detail=await Api.contextEnvelope(envelopeId);
          runtimeDetailMount.appendChild(renderContextHistoryDetail(detail.context||{envelope_id:envelopeId}));
        }catch(e){
          runtimeDetailMount.appendChild(UI.el('div','panel-empty','Context detail unavailable'));
        }
      }

      let envelope={};
      let leanProbe=null;
      let policyDecision=null;
      let diagnostics={};
      let budget={};
      let controlPolicy=null;
      let controlPlane=null;
      try{
        const ctx=await Api.currentContext(opts);
        envelope=(ctx&&ctx.envelope)||{};
        leanProbe=(ctx&&ctx.lean_probe)||null;
        policyDecision=(ctx&&ctx.policy_decision)||null;
        diagnostics=envelope.diagnostics||{};
        budget=envelope.budget||{};
      }catch(e){
        contextSec.appendChild(UI.el('div','panel-empty','Context unavailable'));
      }
      try{
        const cfg=await Api.runtimeEffectiveConfig();
        controlPolicy=(cfg&&cfg.control_policy)||null;
      }catch(e){}
      try{
        controlPlane=await Api.runtimeControlPlane();
      }catch(e){}

      const metrics=UI.el('div','memory-metrics');
      metrics.appendChild(renderMemoryMetric('profile',envelope.profile||profile.value,'mode'));
      metrics.appendChild(renderMemoryMetric('pressure',fmtPressure(diagnostics.pressure_bp),'context'));
      metrics.appendChild(renderMemoryMetric('selected',(envelope.selected||[]).length,'items'));
      metrics.appendChild(renderMemoryMetric('omitted',(envelope.omitted||[]).length,'items'));
      metrics.appendChild(renderMemoryMetric('used',budget.used_tokens||0,'tokens'));
      metrics.appendChild(renderMemoryMetric('stable',shortHash(diagnostics.stable_head_hash),'hash'));
      if(controlPolicy){
        const agent=controlPolicy.agent||{};
        metrics.appendChild(renderMemoryMetric('agents',agent.max_parallel_agents??0,'max'));
      }
      summary.appendChild(metrics);
      summary.appendChild(renderRuntimeControlPlane(controlPlane));
      summary.appendChild(renderLeanProbe(leanProbe,policyDecision));
      if((diagnostics.degraded_sources||[]).length){
        const degraded=UI.el('div','context-degraded');
        degraded.textContent='degraded: '+diagnostics.degraded_sources.join(', ');
        summary.appendChild(degraded);
      }

      const selected=envelope.selected||[];
      if(!selected.length)contextSec.appendChild(UI.el('div','panel-empty','No selected context'));
      selected.slice(0,6).forEach(function(item){contextSec.appendChild(renderContextItem(item))});

      if(!Api.sid){
        runsSec.appendChild(UI.el('div','panel-empty','No active session'));
      }else{
        try{
          const data=await Api.runtimeRuns(Api.sid,{limit:10});
          const rows=data.runs||[];
          const runSummary=((data.tree||{}).summary)||{};
          const runMetrics=UI.el('div','memory-metrics');
          runMetrics.appendChild(renderMemoryMetric('spans',runSummary.span_count||0,'tree'));
          runMetrics.appendChild(renderMemoryMetric('roots',runSummary.root_count||0,'tree'));
          runMetrics.appendChild(renderMemoryMetric('failed',runSummary.failed_count||0,'runs'));
          runMetrics.appendChild(renderMemoryMetric('running',runSummary.running_count||0,'runs'));
          runsSec.appendChild(runMetrics);
          if(!rows.length)runsSec.appendChild(UI.el('div','panel-empty','No runtime runs'));
          rows.slice(-10).reverse().forEach(function(item){runsSec.appendChild(renderRuntimeRunItem(item))});
        }catch(e){runsSec.appendChild(UI.el('div','panel-empty','Runtime runs unavailable'))}
        try{
          const timeline=await Api.runtimeTimeline(Api.sid,{limit:12});
          const events=timeline.events||[];
          const metrics=UI.el('div','memory-metrics');
          metrics.appendChild(renderMemoryMetric('events',timeline.total||0,'timeline'));
          metrics.appendChild(renderMemoryMetric('next',timeline.next_seq??'end','seq'));
          metrics.appendChild(renderMemoryMetric('degraded',timeline.degraded?'yes':'no','state'));
          timelineSec.appendChild(metrics);
          if(timeline.degraded_reason)timelineSec.appendChild(UI.el('div','panel-empty',timeline.degraded_reason));
          if(!events.length)timelineSec.appendChild(UI.el('div','panel-empty','No runtime events'));
          timelineSec.appendChild(renderRuntimeHealthSummary(timeline.health_summary));
          timelineSec.appendChild(renderValueLoopSummary(timeline.value_loop));
          timelineSec.appendChild(renderWorkGraphSummary(events,timeline.workgraph_summary));
          timelineSec.appendChild(renderAgentValueSummary(timeline.agent_value));
          const runtimePolicy=latestPolicyDecision(events);
          if(runtimePolicy.count){
            const policyBox=UI.el('div','agent-workgraph-summary');
            policyBox.innerHTML='<h4>Runtime Control</h4><div class="memory-metrics"></div><small>'+UI.esc('agent '+runtimePolicy.agent+' · review '+runtimePolicy.review+' · signals '+runtimePolicy.signals)+'</small>';
            const policyMetrics=policyBox.querySelector('.memory-metrics');
            policyMetrics.appendChild(renderMemoryMetric('level',runtimePolicy.level,'policy'));
            policyMetrics.appendChild(renderMemoryMetric('score',runtimePolicy.score,'complexity'));
            timelineSec.appendChild(policyBox);
          }
          events.slice(-12).reverse().forEach(function(item){timelineSec.appendChild(renderRuntimeTimelineItem(item,showRuntimeContext))});
          timelineSec.appendChild(runtimeDetailMount);
        }catch(e){timelineSec.appendChild(UI.el('div','panel-empty','Runtime timeline unavailable'))}
      }

      await renderMemoryMaintenance(maintSec);
    }
    refresh.onclick=load;
    input.onkeydown=function(e){if(e.key==='Enter')load()};
    profile.onchange=load;
    await load();
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
    const c=cont();c.innerHTML='<h3>Connectivity & Policy</h3>';
    try{
      const summary=await Api.crossPlaneSummary();
      const identitiesData=await Api.crossPlaneIdentities().catch(function(){return {identities:[]}});
      const grantsData=await Api.crossPlaneGrants().catch(function(){return {grants:[]}});
      const auditData=await Api.crossPlaneAudit().catch(function(){return {records:[]}});
      const adapterData=await Api.crossPlaneActionAdapters().catch(function(){return {capabilities:[]}});
      const executionData=await Api.crossPlaneActionExecutions().catch(function(){return {executions:[]}});
      const sec=UI.el('div','panel-section');
      sec.innerHTML='<h3>Cross-Plane Control</h3>';
      const identities=summary.identity_bindings||{};
      const grants=summary.grants||{};
      const interop=summary.interop||{};
      sec.innerHTML+='<div class="audit-metrics">'
        +'<span><b>'+UI.esc(identities.verified??0)+'</b><small>verified ids</small></span>'
        +'<span><b>'+UI.esc((identities.claimed??0)+(identities.observed??0))+'</b><small>pending ids</small></span>'
        +'<span><b>'+UI.esc(grants.active??0)+'</b><small>active grants</small></span>'
        +'<span><b>'+UI.esc(interop.actions_24h??0)+'</b><small>interop 24h</small></span>'
        +'</div>';
      const detail=UI.el('div','panel-empty');
      detail.textContent='Identities '+((identitiesData.identities||[]).length)+' · Grants '+((grantsData.grants||[]).length)+' · Audit records '+((auditData.records||[]).length);
      sec.appendChild(detail);
      const identityRows=(identitiesData.identities||[]).slice(0,4);
      if(identityRows.length){
        const identityList=UI.el('div','panel-section');
        identityList.innerHTML='<h3>Identity Bindings</h3>';
        identityRows.forEach(function(binding){
          const row=UI.el('div','panel-item audit-record');
          const trust=UI.el('span','audit-source');
          trust.textContent=binding.trust||'unknown';
          const body=UI.el('span','pi-name audit-body');
          const title=UI.el('span','audit-title');
          title.textContent=binding.principal_id||binding.id||'identity';
          const meta=UI.el('span','audit-meta');
          meta.textContent=binding.identity_ref||'';
          body.appendChild(title);
          body.appendChild(meta);
          row.appendChild(trust);
          row.appendChild(body);
          identityList.appendChild(row);
        });
        sec.appendChild(identityList);
      }
      const recentAudit=(auditData.records||[]).slice(0,3);
      if(recentAudit.length){
        const auditList=UI.el('div','panel-section');
        auditList.innerHTML='<h3>Recent Policy Evidence</h3>';
        recentAudit.forEach(function(record){
          const ev=record.evidence||{};
          const row=UI.el('div','panel-item audit-record');
          const source=UI.el('span','audit-source');
          source.textContent=(record.decision&&record.decision.decision)||record.result||'policy';
          const body=UI.el('span','pi-name audit-body');
          const title=UI.el('span','audit-title');
          title.textContent=(record.action&&record.action.requested_capability)||record.summary||'cross-plane action';
          const meta=UI.el('span','audit-meta');
          const parts=[];
          if(ev.policy_version)parts.push(ev.policy_version);
          if(ev.matched_grant_id)parts.push('grant '+ev.matched_grant_id);
          if(ev.consumed_grant_id)parts.push('consumed');
          if(ev.remaining_uses_after!==undefined&&ev.remaining_uses_after!==null)parts.push('remaining '+ev.remaining_uses_after);
          meta.textContent=parts.join(' · ')||'no evidence';
          body.appendChild(title);
          body.appendChild(meta);
          row.appendChild(source);
          row.appendChild(body);
          auditList.appendChild(row);
        });
        sec.appendChild(auditList);
      }
      const capabilities=(adapterData.capabilities||[]).slice(0,4);
      if(capabilities.length){
        const adapterList=UI.el('div','panel-section');
        adapterList.innerHTML='<h3>Adapter Capability</h3>';
        capabilities.forEach(function(capability){
          const row=UI.el('div','panel-item audit-record');
          const source=UI.el('span','audit-source');
          source.textContent=capability.platform||'adapter';
          const body=UI.el('span','pi-name audit-body');
          const title=UI.el('span','audit-title');
          title.textContent=capability.operation||capability.capability||'operation';
          const meta=UI.el('span','audit-meta');
          const parts=[];
          parts.push(capability.live_supported?'live supported':'plan only');
          parts.push(capability.adapter_bound?'bound':'not bound');
          meta.textContent=parts.join(' · ');
          body.appendChild(title);
          body.appendChild(meta);
          row.appendChild(source);
          row.appendChild(body);
          adapterList.appendChild(row);
        });
        sec.appendChild(adapterList);
      }
      const executions=(executionData.executions||[]).slice(0,3);
      if(executions.length){
        const executionList=UI.el('div','panel-section');
        executionList.innerHTML='<h3>Execution Receipts</h3>';
        executions.forEach(function(receipt){
          const row=UI.el('div','panel-item audit-record');
          const source=UI.el('span','audit-source');
          source.textContent=receipt.status||'receipt';
          const body=UI.el('span','pi-name audit-body');
          const title=UI.el('span','audit-title');
          title.textContent=(receipt.action&&receipt.action.requested_capability)||receipt.id||'execution';
          const meta=UI.el('span','audit-meta');
          const parts=[];
          if(receipt.dispatch_status)parts.push(receipt.dispatch_status);
          if(receipt.mode)parts.push(receipt.mode);
          if(receipt.idempotency_key)parts.push('idem '+receipt.idempotency_key);
          meta.textContent=parts.join(' · ')||receipt.id||'';
          body.appendChild(title);
          body.appendChild(meta);
          row.appendChild(source);
          row.appendChild(body);
          executionList.appendChild(row);
          if(receipt.dispatch_target)executionList.appendChild(renderDispatchTarget(receipt.dispatch_target));
          if(receipt.dispatch_outcome)executionList.appendChild(renderDispatchOutcome(receipt.dispatch_outcome));
        });
        sec.appendChild(executionList);
      }
      sec.appendChild(renderCrossPlaneComposer());
      c.appendChild(sec);
    }catch(e){
      c.appendChild(UI.el('div','panel-empty','Cross-plane policy unavailable'));
    }

    const wechatSec=UI.el('div','panel-section');
    wechatSec.innerHTML='<h3>WeChat iLink</h3>';
    c.appendChild(wechatSec);
    try{
      const accounts=await Api.wechatIlinkAccounts().catch(function(){return {accounts:[]}});
      const accountList=accounts.accounts||[];
      const meta=UI.el('div','panel-empty');
      meta.textContent=accountList.length
        ? 'Authorized accounts '+accountList.length+' · active '+(accountList[0].account_id||'').slice(0,18)
        : 'No authorized personal WeChat account';
      wechatSec.appendChild(meta);

      const qrBox=UI.el('div','panel-empty');
      qrBox.style.display='none';
      qrBox.style.alignItems='center';
      qrBox.style.justifyContent='center';
      qrBox.style.background='var(--panel2)';
      qrBox.style.padding='12px';
      wechatSec.appendChild(qrBox);

      const status=UI.el('div','panel-empty','');
      wechatSec.appendChild(status);

      const btn=UI.el('button','btn-secondary');
      btn.textContent='Authorize WeChat';
      btn.onclick=async function(){
        btn.disabled=true;
        status.textContent='Creating QR code...';
        try{
          const qr=await Api.startWechatIlinkQr({bot_type:'3'});
          qrBox.style.display='flex';
          qrBox.innerHTML=qr.qrcode_svg||UI.esc(qr.scan_data||'');
          status.textContent='Scan with WeChat and confirm on phone.';
          const started=Date.now();
          const timer=setInterval(async function(){
            if(Date.now()-started>480000){
              clearInterval(timer);
              btn.disabled=false;
              status.textContent='QR login timed out. Start again.';
              return;
            }
            try{
              const next=await Api.pollWechatIlinkQr({qrcode:qr.qrcode,base_url:qr.base_url});
              if(next.status==='wait') status.textContent='Waiting for scan...';
              else if(next.status==='scaned') status.textContent='Scanned. Confirm in WeChat.';
              else if(next.status==='scaned_but_redirect') status.textContent='Redirecting iLink host...';
              else if(next.status==='confirmed'){
                clearInterval(timer);
                btn.disabled=false;
                status.textContent='Authorized '+((next.account&&next.account.account_id)||'WeChat');
                UI.showToast('WeChat authorized');
                renderGateway();
              }else if(next.status==='expired'){
                clearInterval(timer);
                btn.disabled=false;
                status.textContent='QR expired. Start again.';
              }else{
                status.textContent='Status '+next.status;
              }
            }catch(e){
              clearInterval(timer);
              btn.disabled=false;
              status.textContent='QR poll failed: '+e.message;
            }
          },2000);
        }catch(e){
          btn.disabled=false;
          status.textContent='QR start failed: '+e.message;
        }
      };
      wechatSec.appendChild(btn);
    }catch(e){
      wechatSec.appendChild(UI.el('div','panel-empty','WeChat authorization unavailable'));
    }

    const gatewayTitle=UI.el('div','panel-section');
    gatewayTitle.innerHTML='<h3>Gateway Platforms</h3>';
    c.appendChild(gatewayTitle);
    try{
      const platforms=await Api.listPlatforms();
      if(!platforms||!platforms.length){
        gatewayTitle.appendChild(UI.el('div','panel-empty','No platforms configured.\nEnable feishu/wechat/email in config.yaml'));
        return;
      }
      platforms.forEach(async function(p){
        const sec=UI.el('div','panel-section');
        const platformName=p.name||p;
        sec.innerHTML='<h3>'+UI.esc(platformName)+'</h3>';
        if(typeof p==='object'){
          const metrics=UI.el('div','memory-metrics');
          metrics.appendChild(renderMemoryMetric('status',p.status||'unknown','channel'));
          metrics.appendChild(renderMemoryMetric('enabled',p.enabled?'yes':'no','config'));
          metrics.appendChild(renderMemoryMetric('credential',p.credential_present?'present':'missing','secret'));
          sec.appendChild(metrics);
          if((p.missing_required||[]).length){
            sec.appendChild(UI.el('div','panel-empty','Missing '+p.missing_required.join(', ')));
          }
          if((p.capabilities||[]).length){
            sec.appendChild(UI.el('div','panel-empty','Capabilities '+p.capabilities.join(' · ')));
          }
        }
        c.appendChild(sec);
        try{
          const sessions=await Api.getPlatform(platformName);
          const readiness=sessions&&sessions.readiness;
          if(readiness&&(!p||typeof p!=='object')){
            sec.appendChild(UI.el('div','panel-empty','Status '+(readiness.status||'unknown')));
          }
          if(sessions&&sessions.sessions){
            sessions.sessions.forEach(function(s){
              const item=UI.el('div','panel-item');
              item.innerHTML='<span class="pi-name">'+UI.esc(s.id||'').slice(0,12)+'...</span><span style="font-size:11px;color:var(--text3)"> '+UI.esc(s.title||'')+'</span>';
              sec.appendChild(item);
            });
          }
        }catch(e){sec.appendChild(UI.el('div','panel-empty','Sessions unavailable'))}
      });
    }catch(e){gatewayTitle.appendChild(UI.el('div','panel-empty','Gateway info unavailable'))}
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

  return{renderMemory,renderContext,renderRuntimeConsole,renderMemoryLayer,renderMemorySymbolResults,showMemoryEntryForm,renderMemorySpatial,renderMemoryNetwork,renderProgress,renderSkills,renderCrons,renderSettings,renderAgents,renderTools,renderGateway,renderAudit,renderCCConfig,renderCCProviders,renderCCApproval,renderCCHistory,renderCCUsage};
})();
