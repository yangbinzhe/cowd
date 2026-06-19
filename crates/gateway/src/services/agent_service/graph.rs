use super::*;

impl AgentService {
    pub(crate) async fn enrich_mfg_evidence_agent_graph(
        &self,
        task_service: &TaskService,
        session_service: &SessionService,
        task: &TaskRecord,
        packet: &MatrixEvidencePacket,
    ) -> Result<(TaskRecord, AgentRunGraph), String> {
        let mut graph = task.agent_graph.clone().unwrap_or_else(|| {
            AgentRunGraph::from_objective(task.id.clone(), task.objective.clone())
        });
        enrich_mfg_evidence_agent_graph(&mut graph, packet).map_err(|error| error.to_string())?;
        let task = task_service.upsert_agent_graph(&task.id, graph.clone())?;
        self.append_mfg_agent_runtime_event(session_service, &task, &graph)
            .await?;
        Ok((task, graph))
    }

    pub(crate) async fn plan_mfg_skill_agent_nodes(
        &self,
        task_service: &TaskService,
        session_service: &SessionService,
        incident: &MfgIncident,
        plan: &MfgSkillPlan,
    ) -> Result<Option<AgentRunGraph>, String> {
        let Some(task_id) = incident.task_id.as_deref() else {
            return Ok(None);
        };
        let Some(mut graph) = task_service.agent_graph(task_id)? else {
            return Ok(None);
        };
        let now = now_ms();
        let dependency = if graph.nodes.iter().any(|node| node.id == "mfg_reviewer") {
            "mfg_reviewer"
        } else {
            "planner"
        };
        for skill in &plan.selected_skills {
            let node_id = skill_agent_node_id(&skill.skill_id);
            ensure_agent_node(
                &mut graph,
                AgentTaskNode {
                    id: node_id.clone(),
                    role: AgentRole::Researcher,
                    title: skill.role.clone(),
                    objective: skill.analysis_method.clone(),
                    depends_on: vec![dependency.to_string()],
                    status: AgentNodeStatus::Pending,
                    assigned_agent: Some(skill.skill_id.clone()),
                    result: None,
                    error: None,
                    created_at_ms: now,
                    updated_at_ms: now,
                },
            )
            .map_err(|error| error.to_string())?;
            graph
                .add_evidence(
                    &node_id,
                    "mfg_skill_manifest",
                    format!("mfg:skill:{}", skill.skill_id),
                    format!(
                        "inputs={}, metrics={}, evidence={}",
                        skill.input_fact_types.join(","),
                        skill.input_metric_keys.join(","),
                        skill.required_evidence.join(",")
                    ),
                )
                .map_err(|error| error.to_string())?;
        }
        let task = task_service.upsert_agent_graph(task_id, graph.clone())?;
        self.append_mfg_agent_runtime_event(session_service, &task, &graph)
            .await?;
        Ok(Some(graph))
    }

    pub(crate) async fn complete_mfg_skill_agent_node(
        &self,
        task_service: &TaskService,
        session_service: &SessionService,
        incident: &MfgIncident,
        run: &MfgSkillRun,
    ) -> Result<Option<AgentRunGraph>, String> {
        let Some(task_id) = incident.task_id.as_deref() else {
            return Ok(None);
        };
        let Some(mut graph) = task_service.agent_graph(task_id)? else {
            return Ok(None);
        };
        let node_id = run
            .agent_node_id
            .clone()
            .unwrap_or_else(|| skill_agent_node_id(&run.skill_id));
        if !graph.nodes.iter().any(|node| node.id == node_id) {
            let skill = server_manufacturing_skill_pack()
                .into_iter()
                .find(|skill| skill.skill_id == run.skill_id)
                .ok_or_else(|| format!("MFG skill {} not found", run.skill_id))?;
            let now = now_ms();
            ensure_agent_node(
                &mut graph,
                AgentTaskNode {
                    id: node_id.clone(),
                    role: AgentRole::Researcher,
                    title: skill.role,
                    objective: skill.analysis_method,
                    depends_on: vec!["planner".to_string()],
                    status: AgentNodeStatus::Pending,
                    assigned_agent: Some(run.skill_id.clone()),
                    result: None,
                    error: None,
                    created_at_ms: now,
                    updated_at_ms: now,
                },
            )
            .map_err(|error| error.to_string())?;
        }
        if let Some(node) = graph.nodes.iter_mut().find(|node| node.id == node_id) {
            node.status = AgentNodeStatus::Completed;
            node.result = Some(run.summary.clone());
            node.updated_at_ms = now_ms();
        }
        graph
            .add_evidence(
                &node_id,
                "mfg_skill_run",
                format!("mfg:skill-run:{}:{}", incident.incident_id, run.skill_id),
                run.summary.clone(),
            )
            .map_err(|error| error.to_string())?;
        let task = task_service.upsert_agent_graph(task_id, graph.clone())?;
        self.append_mfg_agent_runtime_event(session_service, &task, &graph)
            .await?;
        Ok(Some(graph))
    }

    async fn append_mfg_agent_runtime_event(
        &self,
        session_service: &SessionService,
        task: &TaskRecord,
        graph: &AgentRunGraph,
    ) -> Result<(), String> {
        self.ensure_mfg_task_session_record(session_service, task)
            .await?;
        session_service
            .append_runtime_event(
                &task.id,
                RuntimeEventScope::Workgraph,
                "mfg.agent_graph.updated",
                serde_json::json!({ "graph": graph }),
            )
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    async fn ensure_mfg_task_session_record(
        &self,
        session_service: &SessionService,
        task: &TaskRecord,
    ) -> Result<(), String> {
        let now = chrono::Utc::now().to_rfc3339();
        let metadata_json = serde_json::json!({
            "kind": "mfg.incident.task",
            "task_id": task.id,
            "objective": task.objective,
            "yolo_mode": task.yolo_mode,
            "current_phase": task.current_phase,
        })
        .to_string();
        let record = SessionRecord {
            session_id: task.id.clone(),
            platform: "mfg".to_string(),
            chat_id: task.id.clone(),
            user_id: None,
            model: None,
            created_at: now.clone(),
            last_activity: now,
            message_count: task.audit.len() as i64,
            reset_policy: "none".to_string(),
            metadata_json: Some(metadata_json),
            input_tokens: 0,
            output_tokens: 0,
            estimated_cost_usd: 0.0,
            status: task.status.as_str().to_string(),
        };
        session_service
            .upsert_stored_session(&record)
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    pub(crate) fn list_agent_graphs(&self, task_service: &TaskService) -> serde_json::Value {
        let runs = task_service.list_agent_graphs().unwrap_or_default();
        serde_json::json!({
            "kind": "agent_run_graphs",
            "runs": runs,
        })
    }

    pub(crate) fn agent_graph(
        &self,
        task_service: &TaskService,
        task_id: &str,
    ) -> Option<AgentRunGraph> {
        task_service.agent_graph(task_id).ok().flatten()
    }

    pub(crate) async fn upsert_agent_graph(
        &self,
        task_service: &TaskService,
        session_service: &SessionService,
        task_id: &str,
        objective: Option<String>,
        nodes: Vec<serde_json::Value>,
    ) -> Result<AgentRunGraph, String> {
        let objective = objective
            .or_else(|| {
                task_service
                    .agent_graph(task_id)
                    .ok()
                    .flatten()
                    .map(|graph| graph.objective)
            })
            .unwrap_or_else(|| "agent run".to_string());
        let mut graph = AgentRunGraph::new(task_id.to_string(), objective);
        for node in nodes {
            let node: AgentTaskNode = serde_json::from_value(node)
                .map_err(|error| format!("invalid agent graph node: {error}"))?;
            graph.add_node(node).map_err(|error| error.to_string())?;
        }
        graph
            .validate_acyclic()
            .map_err(|error| error.to_string())?;
        let task = task_service
            .upsert_agent_graph(task_id, graph.clone())
            .map_err(|error| error.to_string())?;
        session_service
            .append_runtime_event(
                &task.id,
                RuntimeEventScope::Workgraph,
                "agent.run_graph.updated",
                serde_json::json!({ "graph": graph }),
            )
            .await
            .map_err(|error| error.to_string())?;
        Ok(graph)
    }
}

fn enrich_mfg_evidence_agent_graph(
    graph: &mut AgentRunGraph,
    packet: &MatrixEvidencePacket,
) -> Result<(), runtime::AgentGraphError> {
    let now = now_ms();
    ensure_agent_node(
        graph,
        AgentTaskNode {
            id: "mfg_researcher".to_string(),
            role: AgentRole::Researcher,
            title: "MFG Evidence Research".to_string(),
            objective: "Validate MFG evidence packet and identify missing evidence".to_string(),
            depends_on: vec!["planner".to_string()],
            status: AgentNodeStatus::Pending,
            assigned_agent: Some("mfg_researcher".to_string()),
            result: None,
            error: None,
            created_at_ms: now,
            updated_at_ms: now,
        },
    )?;
    ensure_agent_node(
        graph,
        AgentTaskNode {
            id: "mfg_reviewer".to_string(),
            role: AgentRole::Reviewer,
            title: "MFG Insight Review".to_string(),
            objective: "Review confidence, conflicts, and governance readiness".to_string(),
            depends_on: vec!["mfg_researcher".to_string()],
            status: AgentNodeStatus::Pending,
            assigned_agent: Some("mfg_reviewer".to_string()),
            result: None,
            error: None,
            created_at_ms: now,
            updated_at_ms: now,
        },
    )?;
    ensure_agent_node(
        graph,
        AgentTaskNode {
            id: "mfg_merger".to_string(),
            role: AgentRole::Merger,
            title: "MFG Decision Merge".to_string(),
            objective: "Merge agent findings into one governed operating decision".to_string(),
            depends_on: vec!["mfg_reviewer".to_string()],
            status: AgentNodeStatus::Pending,
            assigned_agent: Some("mfg_merger".to_string()),
            result: None,
            error: None,
            created_at_ms: now,
            updated_at_ms: now,
        },
    )?;
    let reference = format!("mfg:evidence:{}", packet.packet_id);
    graph.add_evidence(
        "planner",
        "structured_evidence_packet",
        reference.clone(),
        packet.problem_statement.clone(),
    )?;
    graph.add_evidence(
        "mfg_researcher",
        "structured_evidence_packet",
        reference,
        format!(
            "metric_evidence={}, change_evidence={}, missing_evidence={}",
            packet.metric_evidence.len(),
            packet.change_evidence.len(),
            packet.missing_evidence.len()
        ),
    )?;
    Ok(())
}

fn ensure_agent_node(
    graph: &mut AgentRunGraph,
    node: AgentTaskNode,
) -> Result<(), runtime::AgentGraphError> {
    if graph.nodes.iter().any(|existing| existing.id == node.id) {
        return Ok(());
    }
    graph.add_node(node)
}

pub(super) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}
