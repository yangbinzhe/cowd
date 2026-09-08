impl GatewayToolExecutor {
    async fn retrieve_reality_context(&self, input: &ContextRetrieveRequest, binding: RuntimeToolExecutionBinding<'_>, services: &runtime::RuntimeServices, limit: usize) -> Result<serde_json::Value, ToolError> {
        use sha2::Digest;
        let source=match input.source {ContextRetrieveSource::Fact=>"fact",ContextRetrieveSource::Matrix=>"matrix",_=>return Err(ToolError::new("invalid Reality source"))};
        if input.scope.is_some_and(|s| s != ContextRetrieveScope::Current) || input.session_id.is_some() {
            return Err(ToolError::new("Reality retrieval uses the current Runtime data lease only"));
        }
        let lease=binding.reality_context.ok_or_else(|| ToolError::new("Runtime did not supply an exact Reality data lease"))?;
        lease.validate().map_err(|e|ToolError::new(e.to_string()))?;
        if Some(lease.session_id.as_str())!=binding.session_id {
            return Err(ToolError::new("Reality data lease does not belong to the bound Session"));
        }
        let port=services.reality_recall_port();
        if let Some(reference)=input.entry_ref.as_deref() {
            if input.query.is_some() {return Err(ToolError::new("Reality exact reads do not accept a directory query"));}
            let exact=if source=="fact" {port.read_fact(lease,reference,input.parent_ref.as_deref()).await} else {port.read_matrix(lease,reference).await}.map_err(ToolError::new)?;
            let page=evidence_content_page(&exact.content,&exact.sha256,&EvidenceRetrieveToolRequest {evidence_ref:reference.to_owned(),query:None,limit:Some(limit),cursor:input.content_cursor.clone()})?;
            let next_request=page["next_cursor"].as_str().map(|cursor| compact_context_request(serde_json::json!({"source":source,"entry_ref":reference,"parent_ref":input.parent_ref,"content_cursor":cursor,"limit":limit})));
            let related_reads=exact.related_read_refs.into_iter().map(|reference_to_read| serde_json::json!({
                "entry_ref":reference_to_read,"read_request":compact_context_request(serde_json::json!({"source":source,"entry_ref":reference_to_read,"parent_ref":(source=="fact").then_some(reference)}))
            })).collect::<Vec<_>>();
            Ok(serde_json::json!({"kind":"runtime.context_retrieval","source":source,"status":"completed",
                "chunks":page["chunks"],"entry_ref":exact.source_ref,"sha256":exact.sha256,"scope":exact.scope,
                "source_time":exact.source_time,"encoding":"utf8_json","content_truncated":page["truncated"],
                "next_request":next_request,"evidence_reads":related_reads}))
        } else {
            if input.parent_ref.is_some() {return Err(ToolError::new("parent_ref requires an exact Fact evidence read"));}
            let granted=if source=="fact" {!lease.fact_refs.is_empty() || !lease.fact_boundaries.is_empty()} else {!lease.matrix_snapshot_refs.is_empty()};
            if !granted {return Ok(serde_json::json!({"kind":"runtime.context_retrieval","source":source,"status":"disabled","selected":[],"reason":"current Runtime data lease grants no references or boundaries for this source"}));}
            let query=input.query.as_deref().unwrap_or("");
            let (selected,next_cursor)=if source=="fact" {
                let page=port.discover_facts(lease,query,input.cursor.as_deref(),limit).await.map_err(ToolError::new)?;
                let selected=page.records.into_iter().map(|fact| {
                    let reference=format!("fact:{}",fact.id.as_str());
                    let bytes=serde_json::to_vec(&serde_json::to_value(&fact).map_err(|e|ToolError::new(e.to_string()))?).map_err(|e|ToolError::new(e.to_string()))?;
                    Ok(serde_json::json!({"entry_ref":reference,"ref":reference,"source_kind":"fact","scope":fact.scope_key,
                        "sha256":format!("sha256:{:x}",sha2::Sha256::digest(bytes)),"source_time":fact.updated_at,
                        "preview":fact.statement.chars().take(480).collect::<String>(),"boundary":fact.boundary,
                        "information_status":fact.status,"support":fact.support,"evidence_completeness":fact.evidence_completeness,
                        "read_request":{"source":source,"entry_ref":reference}}))
                }).collect::<Result<Vec<_>,ToolError>>()?;
                (selected,page.next_cursor)
            } else {
                let page=port.discover_matrix(lease,query,input.cursor.as_deref(),limit).await.map_err(ToolError::new)?;
                let selected=page.records.into_iter().map(|record| {
                    let reference=record.reference();let payload=record.value().map_err(|e|ToolError::new(e.to_string()))?;
                    let content=serde_json::to_string(&payload).map_err(|e|ToolError::new(e.to_string()))?;
                    let (kind,preview)=match &record {
                        matrix_repository::MatrixCatalogRecord::Fact(fact)=>("matrix_fact",format!("{}: {} {}",fact.fact_type,fact.dimensions,fact.measures)),
                        matrix_repository::MatrixCatalogRecord::SourceSnapshot(snapshot)=>("matrix_source_snapshot",format!("{} {:?} {} rows",snapshot.source_system,snapshot.source_kind,snapshot.row_count)),
                    };
                    Ok(serde_json::json!({"entry_ref":reference,"ref":reference,"source_kind":kind,"scope":format!("matrix:source_snapshot:{}",record.snapshot_id()),
                        "sha256":format!("sha256:{:x}",sha2::Sha256::digest(content.as_bytes())),"source_time":record.source_time(),
                        "preview":preview.chars().take(480).collect::<String>(),"information_status":"persisted","read_request":{"source":source,"entry_ref":reference}}))
                }).collect::<Result<Vec<_>,ToolError>>()?;
                (selected,page.next_cursor)
            };
            let next_request=next_cursor.as_ref().map(|cursor| compact_context_request(serde_json::json!({"source":source,"query":input.query,"cursor":cursor,"limit":limit})));
            Ok(serde_json::json!({"kind":"runtime.context_retrieval","source":source,"status":"completed","selected":selected,"next_cursor":next_cursor,"next_request":next_request,
                "coverage":{"ordering":"stable_source_reference","record_set":"first_page_creation_snapshot","complete":next_request.is_none()},"scope":"current_data_lease"}))
        }
    }
}
