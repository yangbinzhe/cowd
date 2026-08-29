// @generated adapter inventory. Public method/path identity lives in surface.
use surface::gateway_api::{routes, GatewayRouteSpec};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct GatewayRouteBinding {
    pub(crate) route: GatewayRouteSpec,
    pub(crate) source: &'static str,
    pub(crate) handler: &'static str,
}

pub(crate) const GATEWAY_ROUTE_BINDINGS: &[GatewayRouteBinding] = &[
    GatewayRouteBinding {
        route: routes::DELETE_API_APPS_BY_APP_ID_SUBSCRIPTIONS_BY_SUBSCRIPTION_ID,
        source: "app_routes.rs",
        handler: "cancel_subscription",
    },
    GatewayRouteBinding {
        route: routes::DELETE_API_CROSS_PLANE_GRANTS_BY_ID,
        source: "cross_plane_routes.rs",
        handler: "cross_plane_revoke_grant_handler",
    },
    GatewayRouteBinding {
        route: routes::DELETE_API_CROSS_PLANE_IDENTITIES_BY_ID,
        source: "cross_plane_routes.rs",
        handler: "cross_plane_revoke_identity_handler",
    },
    GatewayRouteBinding {
        route: routes::DELETE_API_MEMORY_BY_LAYER_BY_ID,
        source: "memory_routes.rs",
        handler: "delete_memory_entry_handler",
    },
    GatewayRouteBinding {
        route: routes::DELETE_API_MISSION_SCHEDULES_BY_ID,
        source: "mission_routes.rs",
        handler: "delete_mission_schedule_handler",
    },
    GatewayRouteBinding {
        route: routes::DELETE_API_PROFILES_BY_ID,
        source: "profile_routes.rs",
        handler: "delete_profile_handler",
    },
    GatewayRouteBinding {
        route: routes::DELETE_API_RUNTIME_LIVE_SUBSCRIPTIONS_BY_ID,
        source: "live_routes.rs",
        handler: "delete_live_subscription",
    },
    GatewayRouteBinding {
        route: routes::DELETE_API_RUNTIME_MANAGED_AGENTS_DEFINITIONS_BY_ID,
        source: "managed_agent_routes.rs",
        handler: "managed_agent_definition_delete_handler",
    },
    GatewayRouteBinding {
        route: routes::DELETE_API_SESSIONS_BY_ID,
        source: "session_routes.rs",
        handler: "delete_session",
    },
    GatewayRouteBinding {
        route: routes::DELETE_API_SESSIONS_BY_ID_ATTACHMENTS_BY_REF_ID,
        source: "workspace_routes.rs",
        handler: "delete_session_attachment_handler",
    },
    GatewayRouteBinding {
        route: routes::DELETE_API_SESSIONS_BY_ID_MISSION_FOCUS,
        source: "session_routes.rs",
        handler: "clear_mission_focus_handler",
    },
    GatewayRouteBinding {
        route: routes::DELETE_API_SESSIONS_BY_ID_TASK_FOCUS,
        source: "session_routes.rs",
        handler: "clear_task_focus_handler",
    },
    GatewayRouteBinding {
        route: routes::DELETE_API_SKILLS_BY_ID,
        source: "skill_routes.rs",
        handler: "skill_delete_handler",
    },
    GatewayRouteBinding {
        route: routes::DELETE_API_WORKSPACE_FILES,
        source: "workspace_routes.rs",
        handler: "delete_workspace_path_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_AGENTS_CATALOG,
        source: "agent_routes.rs",
        handler: "agent_catalog_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_AGENTS_DIRECTORY,
        source: "agent_routes.rs",
        handler: "agent_directory_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_AGENTS_DISCOVER,
        source: "agent_routes.rs",
        handler: "agent_discover_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_AGENTS_EXECUTION_GRAPHS,
        source: "agent_routes.rs",
        handler: "execution_graphs_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_AGENTS_SELF_MODELS,
        source: "agent_routes.rs",
        handler: "agent_self_models_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_APPROVAL_BY_ID,
        source: "approval_routes.rs",
        handler: "approval_exact_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_APPROVAL_CONFIG,
        source: "approval_routes.rs",
        handler: "approval_config_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_APPROVAL_GRANTS,
        source: "approval_routes.rs",
        handler: "approval_grants_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_APPROVAL_HISTORY,
        source: "approval_routes.rs",
        handler: "approval_history_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_APPROVAL_PENDING,
        source: "approval_routes.rs",
        handler: "approval_pending_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_APPS,
        source: "app_routes.rs",
        handler: "list_apps",
    },
    GatewayRouteBinding {
        route: routes::GET_API_APPS_BY_APP_ID,
        source: "app_routes.rs",
        handler: "get_app",
    },
    GatewayRouteBinding {
        route: routes::GET_API_APPS_BY_APP_ID_LOGS,
        source: "app_routes.rs",
        handler: "get_app_logs",
    },
    GatewayRouteBinding {
        route: routes::GET_API_APPS_BY_APP_ID_RECEIPTS_BY_RECEIPT_ID,
        source: "app_routes.rs",
        handler: "get_receipt",
    },
    GatewayRouteBinding {
        route: routes::GET_API_AUDIT_EXPORT,
        source: "audit_routes.rs",
        handler: "audit_export_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_AUTH_VERIFY,
        source: "public_routes.rs",
        handler: "verify_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_CONFIG,
        source: "system_routes.rs",
        handler: "config_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_CONFIG_PROVIDER_CATALOG,
        source: "system_routes.rs",
        handler: "config_provider_catalog_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_CONFIG_PROVIDERS,
        source: "system_routes.rs",
        handler: "config_providers_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_CONNECTORS_ACCOUNTS,
        source: "connector_routes.rs",
        handler: "connector_accounts_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_CONNECTORS_CAPABILITIES,
        source: "connector_routes.rs",
        handler: "connector_capabilities_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_CONNECTORS_MCP_SERVERS,
        source: "connector_routes.rs",
        handler: "mcp_servers_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_CONNECTORS_RESOURCES,
        source: "connector_routes.rs",
        handler: "connector_resources_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_CONNECTORS_SERVICES,
        source: "connector_routes.rs",
        handler: "connector_services_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_CONNECTORS_SERVICES_BY_SERVICE_ID_TOOLS,
        source: "connector_routes.rs",
        handler: "connector_service_tools_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_CONNECTORS_SOURCES,
        source: "connector_routes.rs",
        handler: "connector_sources_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_CONNECTORS_SOURCES_BY_ADAPTER_ID_STATE,
        source: "connector_routes.rs",
        handler: "connector_source_state_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_CONNECTORS_SUMMARY,
        source: "connector_routes.rs",
        handler: "connector_summary_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_CONTEXT_BY_ENVELOPE_ID,
        source: "context_routes.rs",
        handler: "get_context_envelope_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_CONTEXT_CURRENT,
        source: "context_routes.rs",
        handler: "context_current_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_COWD_CAPABILITIES,
        source: "core_routes.rs",
        handler: "capabilities_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_COWD_PROJECTION,
        source: "core_routes.rs",
        handler: "projection_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_COWD_RELEASE_GATE,
        source: "core_routes.rs",
        handler: "release_gate_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_COWD_STRUCTURED_EVIDENCE,
        source: "core_routes.rs",
        handler: "structured_evidence_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_COWD_STRUCTURED_FACTS,
        source: "core_routes.rs",
        handler: "structured_facts_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_COWD_STRUCTURED_SOURCES,
        source: "core_routes.rs",
        handler: "structured_sources_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_COWD_STRUCTURED_SOURCES_BY_ID,
        source: "core_routes.rs",
        handler: "structured_source_get_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_COWD_STRUCTURED_WATERMARKS,
        source: "core_routes.rs",
        handler: "structured_watermarks_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_COWD_SURFACES,
        source: "core_routes.rs",
        handler: "surfaces_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_CROSS_PLANE_ACTION_ADAPTERS,
        source: "cross_plane_routes.rs",
        handler: "cross_plane_action_adapters_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_CROSS_PLANE_ACTION_EXECUTIONS,
        source: "cross_plane_routes.rs",
        handler: "cross_plane_action_executions_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_CROSS_PLANE_ACTION_EXECUTIONS_BY_ID,
        source: "cross_plane_routes.rs",
        handler: "cross_plane_action_execution_get_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_CROSS_PLANE_AUDIT,
        source: "cross_plane_routes.rs",
        handler: "cross_plane_audit_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_CROSS_PLANE_GRANTS,
        source: "cross_plane_routes.rs",
        handler: "cross_plane_grants_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_CROSS_PLANE_IDENTITIES,
        source: "cross_plane_routes.rs",
        handler: "cross_plane_identities_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_CROSS_PLANE_SUMMARY,
        source: "cross_plane_routes.rs",
        handler: "cross_plane_summary_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_EDGES,
        source: "edge_routes.rs",
        handler: "edge_registry_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_EDGES_CONNECTORS,
        source: "edge_routes.rs",
        handler: "edge_connectors_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_EDGES_CONNECTORS_MESSAGE,
        source: "edge_routes.rs",
        handler: "edge_message_connectors_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_EDGES_CONNECTORS_SOURCE,
        source: "edge_routes.rs",
        handler: "edge_source_connectors_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_EDGES_HEALTH,
        source: "edge_routes.rs",
        handler: "edge_health_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_EDGES_SURFACES,
        source: "edge_routes.rs",
        handler: "edge_surfaces_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_EVIDENCE_PROJECTIONS,
        source: "context_routes.rs",
        handler: "list_evidence_audit_projections",
    },
    GatewayRouteBinding {
        route: routes::GET_API_EVIDENCE_RESOLVE,
        source: "context_routes.rs",
        handler: "resolve_evidence_ref_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_EVOLUTION_CANDIDATES,
        source: "evolution_routes.rs",
        handler: "evolution_candidates_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_EVOLUTION_CANDIDATES_BY_ID,
        source: "evolution_routes.rs",
        handler: "evolution_candidate_detail_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_EVOLUTION_CASES,
        source: "evolution_routes.rs",
        handler: "evolution_cases_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_EVOLUTION_CASES_BY_ID,
        source: "evolution_routes.rs",
        handler: "evolution_case_detail_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_EVOLUTION_CHAIN_BY_ID,
        source: "evolution_routes.rs",
        handler: "evolution_chain_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_EVOLUTION_COLLABORATION_PATTERNS,
        source: "evolution_routes.rs",
        handler: "evolution_collaboration_patterns_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_EVOLUTION_DIAGNOSES,
        source: "evolution_routes.rs",
        handler: "evolution_diagnoses_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_EVOLUTION_EVALUATION_POLICY,
        source: "evolution_routes.rs",
        handler: "evolution_evaluation_policy_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_EVOLUTION_EVALUATION_POLICY_REVIEWS,
        source: "evolution_routes.rs",
        handler: "evolution_evaluation_policy_reviews_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_EVOLUTION_MISSIONS_BY_ID_DETAIL,
        source: "evolution_routes.rs",
        handler: "evolution_mission_detail_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_EVOLUTION_MISSIONS_SUMMARY,
        source: "evolution_routes.rs",
        handler: "evolution_missions_summary_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_EVOLUTION_OVERVIEW,
        source: "evolution_routes.rs",
        handler: "evolution_overview_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_EVOLUTION_PROPOSALS,
        source: "evolution_routes.rs",
        handler: "evolution_proposals_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_EVOLUTION_PROPOSALS_BY_ID,
        source: "evolution_routes.rs",
        handler: "evolution_proposal_detail_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_EVOLUTION_PROPOSALS_BY_ID_SKILL_DRAFT,
        source: "evolution_routes.rs",
        handler: "evolution_skill_draft_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_EVOLUTION_REVIEWS,
        source: "evolution_routes.rs",
        handler: "evolution_reviews_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_EVOLUTION_REVIEWS_BY_ID,
        source: "evolution_routes.rs",
        handler: "evolution_review_detail_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_EVOLUTION_SIGNALS,
        source: "evolution_routes.rs",
        handler: "evolution_signals_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_FILE_RAW,
        source: "workspace_routes.rs",
        handler: "raw_workspace_file_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_GATEWAY_CAPABILITY_CONTRACT,
        source: "public_routes.rs",
        handler: "capability_contract_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_GATEWAY_OPENAI_TOOLS,
        source: "public_routes.rs",
        handler: "openai_tools_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_GATEWAY_OPENAPI_JSON,
        source: "public_routes.rs",
        handler: "openapi_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_GATEWAY_ROUTE_MANIFEST,
        source: "public_routes.rs",
        handler: "route_manifest_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_GROWTH_EVENTS,
        source: "growth_routes.rs",
        handler: "growth_events_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_GROWTH_STATUS,
        source: "growth_routes.rs",
        handler: "growth_status_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_HARNESS_EVAL_REPORTS,
        source: "harness_eval_routes.rs",
        handler: "harness_eval_reports_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_HARNESS_EVAL_REPORTS_BY_ID,
        source: "harness_eval_routes.rs",
        handler: "harness_eval_report_detail_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_HARNESS_EVAL_REPORTS_BY_ID_ARTIFACTS,
        source: "harness_eval_routes.rs",
        handler: "harness_eval_report_artifacts_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_HARNESS_EVAL_REPORTS_BY_ID_GATE,
        source: "harness_eval_routes.rs",
        handler: "harness_eval_report_gate_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_HARNESS_EVAL_REPORTS_LATEST,
        source: "harness_eval_routes.rs",
        handler: "harness_eval_latest_report_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_HARNESS_EVAL_RUNS,
        source: "harness_eval_routes.rs",
        handler: "harness_eval_runs_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_HARNESS_EVAL_RUNS_BY_ID,
        source: "harness_eval_routes.rs",
        handler: "harness_eval_run_detail_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_HARNESS_EVAL_SCENARIOS,
        source: "harness_eval_routes.rs",
        handler: "harness_eval_scenarios_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MATRIX_ATTENTION_HOT,
        source: "matrix_routes.rs",
        handler: "matrix_attention_hot_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MATRIX_CHANGES,
        source: "matrix_routes.rs",
        handler: "matrix_changes_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MATRIX_COMPUTE_JOBS_BY_ID,
        source: "matrix_routes.rs",
        handler: "matrix_compute_job_get_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MATRIX_CONNECTOR_RUNS_BY_ID,
        source: "matrix_routes.rs",
        handler: "matrix_connector_run_get_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MATRIX_DATA_PLANE_HEALTH,
        source: "matrix_routes.rs",
        handler: "matrix_data_plane_health_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MATRIX_ENTITIES,
        source: "matrix_routes.rs",
        handler: "matrix_entities_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MATRIX_ENTITIES_BY_ID,
        source: "matrix_routes.rs",
        handler: "matrix_entity_get_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MATRIX_ENTITIES_BY_ID_IMPACT_PATH,
        source: "matrix_routes.rs",
        handler: "matrix_entity_impact_path_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MATRIX_ENTITIES_BY_ID_RELATIONS,
        source: "matrix_routes.rs",
        handler: "matrix_entity_relations_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MATRIX_EVIDENCE_BY_ID,
        source: "matrix_routes.rs",
        handler: "matrix_evidence_get_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MATRIX_EVIDENCE_BY_ID_CONTEXT,
        source: "matrix_routes.rs",
        handler: "matrix_evidence_context_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MATRIX_HEALTH,
        source: "matrix_routes.rs",
        handler: "matrix_health_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MATRIX_METRICS,
        source: "matrix_routes.rs",
        handler: "matrix_metrics_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MATRIX_METRICS_BY_ID,
        source: "matrix_routes.rs",
        handler: "matrix_metric_detail_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MATRIX_METRICS_BY_ID_LINEAGE,
        source: "matrix_routes.rs",
        handler: "matrix_metric_lineage_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MATRIX_QUALITY_GATES_BY_ID,
        source: "matrix_routes.rs",
        handler: "matrix_quality_gate_get_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MATRIX_SOURCE_PACKS_BY_ID,
        source: "matrix_routes.rs",
        handler: "matrix_source_pack_get_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MATRIX_SOURCE_PACKS_BY_ID_SNAPSHOTS,
        source: "matrix_routes.rs",
        handler: "matrix_source_pack_snapshots_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MATRIX_SOURCE_SNAPSHOTS_BY_ID,
        source: "matrix_routes.rs",
        handler: "matrix_source_snapshot_get_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MEMORY,
        source: "memory_routes.rs",
        handler: "memory_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MEMORY_BY_LAYER,
        source: "memory_routes.rs",
        handler: "memory_layer_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MEMORY_CLUSTERS,
        source: "memory_routes.rs",
        handler: "memory_clusters_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MEMORY_CONTEXT_ENVELOPE,
        source: "memory_routes.rs",
        handler: "memory_context_envelope_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MEMORY_CONTEXT_ENVELOPE_BY_SESSION_ID,
        source: "memory_routes.rs",
        handler: "memory_context_envelope_session_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MEMORY_ENTITIES,
        source: "memory_routes.rs",
        handler: "memory_entities_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MEMORY_GRAPH,
        source: "memory_routes.rs",
        handler: "memory_graph_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MEMORY_KNOWLEDGE,
        source: "memory_routes.rs",
        handler: "memory_knowledge_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MEMORY_KNOWLEDGE_CANDIDATES,
        source: "memory_routes.rs",
        handler: "memory_knowledge_candidates_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MEMORY_KNOWLEDGE_CANDIDATES_BY_ID,
        source: "memory_routes.rs",
        handler: "memory_knowledge_candidate_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MEMORY_KNOWLEDGE_CONFLICTS,
        source: "memory_routes.rs",
        handler: "memory_knowledge_conflicts_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MEMORY_KNOWLEDGE_HEALTH,
        source: "memory_routes.rs",
        handler: "memory_knowledge_health_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MEMORY_KNOWLEDGE_MAINTENANCE,
        source: "memory_routes.rs",
        handler: "memory_knowledge_maintenance_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MEMORY_KNOWLEDGE_NAMESPACES,
        source: "memory_routes.rs",
        handler: "memory_knowledge_namespaces_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MEMORY_LAYERS,
        source: "memory_routes.rs",
        handler: "memory_layers_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MEMORY_LIFECYCLE_BY_ID,
        source: "memory_routes.rs",
        handler: "memory_lifecycle_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MEMORY_LINKS,
        source: "memory_routes.rs",
        handler: "memory_links_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MEMORY_MAINTENANCE,
        source: "memory_routes.rs",
        handler: "memory_maintenance_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MEMORY_PACKET,
        source: "memory_routes.rs",
        handler: "memory_packet_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MEMORY_PERFORMANCE,
        source: "memory_routes.rs",
        handler: "performance_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MEMORY_RECALL_EXPLAIN,
        source: "memory_routes.rs",
        handler: "memory_recall_explain_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MEMORY_RUNTIME,
        source: "memory_routes.rs",
        handler: "memory_runtime_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MEMORY_SEARCH,
        source: "memory_routes.rs",
        handler: "memory_search_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MEMORY_STATS,
        source: "memory_routes.rs",
        handler: "memory_stats_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MEMORY_STATUS,
        source: "memory_routes.rs",
        handler: "memory_status_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MEMORY_SYMBOL_LINKS,
        source: "memory_routes.rs",
        handler: "memory_symbol_links_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MEMORY_TRIPLES,
        source: "memory_routes.rs",
        handler: "memory_triples_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MESSAGE_BINDINGS,
        source: "message_connector_routes.rs",
        handler: "list_message_bindings_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MESSAGE_CONNECTORS,
        source: "message_connector_routes.rs",
        handler: "list_message_connectors_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MESSAGE_CONNECTORS_BY_NAME_STATUS,
        source: "message_connector_routes.rs",
        handler: "get_message_connector_status_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MESSAGE_CONNECTORS_WECHAT_ILINK_ACCOUNTS,
        source: "message_connector_routes.rs",
        handler: "wechat_ilink_accounts_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MESSAGE_ENDPOINTS,
        source: "message_connector_routes.rs",
        handler: "list_message_endpoints_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MESSAGE_ROUTES,
        source: "message_connector_routes.rs",
        handler: "list_message_routes_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MISSION_APPROVALS,
        source: "mission_routes.rs",
        handler: "mission_approvals_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MISSION_CONFLICTS,
        source: "mission_routes.rs",
        handler: "mission_conflicts_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MISSION_CONTROL,
        source: "mission_routes.rs",
        handler: "mission_control_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MISSION_CONTROL_AGENTS_BY_AGENT_ID_EVENTS,
        source: "mission_routes.rs",
        handler: "agent_mission_events_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MISSION_CONTROL_DELTA,
        source: "mission_routes.rs",
        handler: "mission_control_delta_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MISSION_CONTROL_SUMMARY,
        source: "mission_routes.rs",
        handler: "mission_control_summary_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MISSION_CONTROL_TEAMS,
        source: "mission_routes.rs",
        handler: "collaboration_runs_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MISSION_CONTROL_TEAMS_BY_TEAM_ID_EVIDENCE,
        source: "mission_routes.rs",
        handler: "team_mission_evidence_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MISSION_CONTROL_TEAMS_BY_TEAM_ID_EXECUTION,
        source: "mission_routes.rs",
        handler: "team_execution_plan_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MISSION_CONTROL_TEAMS_BY_TEAM_ID_RUN,
        source: "mission_routes.rs",
        handler: "collaboration_run_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MISSION_PROJECTION,
        source: "mission_routes.rs",
        handler: "mission_projection_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MISSION_RELATIONS,
        source: "mission_routes.rs",
        handler: "mission_relations_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MISSION_SCHEDULES,
        source: "mission_routes.rs",
        handler: "mission_schedules_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MISSION_SESSIONS,
        source: "mission_routes.rs",
        handler: "mission_projection_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_MISSION_SESSIONS_BY_ID,
        source: "mission_routes.rs",
        handler: "mission_session_detail_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_PLATFORMS,
        source: "message_connector_routes.rs",
        handler: "list_platforms_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_PLATFORMS_BY_NAME,
        source: "message_connector_routes.rs",
        handler: "get_platform_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_PROFILES,
        source: "profile_routes.rs",
        handler: "profiles_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_REALITY_BOUNDARIES,
        source: "reality_routes.rs",
        handler: "reality_boundaries_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_REALITY_CAPABILITIES,
        source: "reality_routes.rs",
        handler: "reality_capabilities_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_REALITY_CONTEXT_ENVELOPE,
        source: "reality_routes.rs",
        handler: "reality_context_envelope_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_REALITY_EVIDENCE_BY_ID,
        source: "reality_routes.rs",
        handler: "reality_evidence_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_REALITY_FLOW,
        source: "reality_routes.rs",
        handler: "reality_flow_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_REALITY_GOVERNANCE,
        source: "reality_routes.rs",
        handler: "reality_governance_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_REALITY_PROMOTIONS,
        source: "reality_routes.rs",
        handler: "reality_promotions_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_REALITY_RECALL_REPORT,
        source: "reality_routes.rs",
        handler: "reality_recall_report_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_REALITY_STATIC,
        source: "reality_routes.rs",
        handler: "reality_static_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_REALITY_STATUS,
        source: "reality_routes.rs",
        handler: "reality_status_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_RESOURCES_BY_ID,
        source: "resource_routes.rs",
        handler: "get_resource_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_RESOURCES_BY_ID_CONTENT,
        source: "resource_routes.rs",
        handler: "get_resource_content_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_RESOURCES_BY_ID_EVIDENCE,
        source: "resource_routes.rs",
        handler: "get_resource_evidence_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_RUNTIME_AGENTS,
        source: "agent_routes.rs",
        handler: "runtime_agents_list_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_RUNTIME_AGENTS_BY_ID,
        source: "agent_routes.rs",
        handler: "runtime_agent_detail_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_RUNTIME_AGENTS_BY_ID_EVENTS,
        source: "agent_routes.rs",
        handler: "runtime_agent_events_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_RUNTIME_CAPABILITIES,
        source: "runtime_routes.rs",
        handler: "get_runtime_capabilities",
    },
    GatewayRouteBinding {
        route: routes::GET_API_RUNTIME_CONFIG_EFFECTIVE,
        source: "runtime_routes.rs",
        handler: "get_runtime_effective_config",
    },
    GatewayRouteBinding {
        route: routes::GET_API_RUNTIME_CONFIG_RELOAD_STATUS,
        source: "runtime_routes.rs",
        handler: "get_runtime_config_reload_status",
    },
    GatewayRouteBinding {
        route: routes::GET_API_RUNTIME_CONTROL_PLANE,
        source: "runtime_routes.rs",
        handler: "get_runtime_control_plane",
    },
    GatewayRouteBinding {
        route: routes::GET_API_RUNTIME_EVENTS,
        source: "runtime_routes.rs",
        handler: "get_runtime_events",
    },
    GatewayRouteBinding {
        route: routes::GET_API_RUNTIME_EVENTS_REPLAY_REPORT,
        source: "runtime_routes.rs",
        handler: "get_runtime_events_replay_report",
    },
    GatewayRouteBinding {
        route: routes::GET_API_RUNTIME_EXECUTIONS_BY_ID,
        source: "route_registry.rs",
        handler: "get_execution_projection",
    },
    GatewayRouteBinding {
        route: routes::GET_API_RUNTIME_EXECUTIONS_BY_ID_ACTIVITY,
        source: "route_registry.rs",
        handler: "get_execution_activity",
    },
    GatewayRouteBinding {
        route: routes::GET_API_RUNTIME_LIVE_BY_ID,
        source: "live_routes.rs",
        handler: "get_live_stream",
    },
    GatewayRouteBinding {
        route: routes::GET_API_RUNTIME_MANAGED_AGENTS,
        source: "managed_agent_routes.rs",
        handler: "managed_agent_projection_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_RUNTIME_MANAGED_AGENTS_DEFINITIONS,
        source: "managed_agent_routes.rs",
        handler: "managed_agent_definitions_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_RUNTIME_MANAGED_AGENTS_EFFECTS,
        source: "managed_agent_routes.rs",
        handler: "managed_agent_effects_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_RUNTIME_OUTBOX,
        source: "runtime_routes.rs",
        handler: "get_runtime_outbox_status",
    },
    GatewayRouteBinding {
        route: routes::GET_API_RUNTIME_SESSION_LEASES,
        source: "runtime_routes.rs",
        handler: "get_runtime_session_leases",
    },
    GatewayRouteBinding {
        route: routes::GET_API_RUNTIME_SNAPSHOT,
        source: "runtime_routes.rs",
        handler: "get_runtime_snapshot",
    },
    GatewayRouteBinding {
        route: routes::GET_API_RUNTIME_SOURCE_AUDIT,
        source: "runtime_routes.rs",
        handler: "get_runtime_source_audit",
    },
    GatewayRouteBinding {
        route: routes::GET_API_RUNTIME_SOURCE_REPAIR_PLAN,
        source: "runtime_routes.rs",
        handler: "get_runtime_source_repair_plan",
    },
    GatewayRouteBinding {
        route: routes::GET_API_RUNTIME_STATUS,
        source: "runtime_routes.rs",
        handler: "get_runtime_status",
    },
    GatewayRouteBinding {
        route: routes::GET_API_RUNTIME_TEAMS_BY_ID_WORKING_STATE,
        source: "agent_routes.rs",
        handler: "team_working_state_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_RUNTIME_TIMELINE,
        source: "runtime_routes.rs",
        handler: "get_runtime_timeline",
    },
    GatewayRouteBinding {
        route: routes::GET_API_RUNTIME_TURNS,
        source: "runtime_routes.rs",
        handler: "get_runtime_turns",
    },
    GatewayRouteBinding {
        route: routes::GET_API_RUNTIME_TURNS_BY_ID,
        source: "runtime_routes.rs",
        handler: "get_runtime_turn",
    },
    GatewayRouteBinding {
        route: routes::GET_API_RUNTIME_UPGRADE_INVENTORY,
        source: "runtime_routes.rs",
        handler: "get_upgrade_inventory",
    },
    GatewayRouteBinding {
        route: routes::GET_API_RUNTIME_UPGRADE_MAINTENANCE,
        source: "runtime_routes.rs",
        handler: "get_upgrade_maintenance",
    },
    GatewayRouteBinding {
        route: routes::GET_API_SESSIONS,
        source: "session_routes.rs",
        handler: "list_sessions",
    },
    GatewayRouteBinding {
        route: routes::GET_API_SESSIONS_BY_ID,
        source: "session_routes.rs",
        handler: "get_session",
    },
    GatewayRouteBinding {
        route: routes::GET_API_SESSIONS_BY_ID_ATTACHMENTS,
        source: "workspace_routes.rs",
        handler: "list_session_attachments_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_SESSIONS_BY_ID_CONTEXT,
        source: "context_routes.rs",
        handler: "get_session_context_history",
    },
    GatewayRouteBinding {
        route: routes::GET_API_SESSIONS_BY_ID_CONTEXT_RECOMMENDATIONS,
        source: "context_routes.rs",
        handler: "get_context_recommendation_stats",
    },
    GatewayRouteBinding {
        route: routes::GET_API_SESSIONS_BY_ID_EVENTS,
        source: "session_routes.rs",
        handler: "get_session_events",
    },
    GatewayRouteBinding {
        route: routes::GET_API_SESSIONS_BY_ID_EVIDENCE,
        source: "session_routes.rs",
        handler: "get_session_evidence",
    },
    GatewayRouteBinding {
        route: routes::GET_API_SESSIONS_BY_ID_EXECUTION,
        source: "session_routes.rs",
        handler: "get_session_execution_index",
    },
    GatewayRouteBinding {
        route: routes::GET_API_SESSIONS_BY_ID_EXECUTION_POLICY,
        source: "session_routes.rs",
        handler: "get_session_execution_policy",
    },
    GatewayRouteBinding {
        route: routes::GET_API_SESSIONS_BY_ID_EXECUTION_LIVE,
        source: "session_routes.rs",
        handler: "get_session_execution_live",
    },
    GatewayRouteBinding {
        route: routes::GET_API_SESSIONS_BY_ID_HISTORY_INDEX,
        source: "session_routes.rs",
        handler: "get_session_history_index",
    },
    GatewayRouteBinding {
        route: routes::GET_API_SESSIONS_BY_ID_INPUT_PROJECTION,
        source: "message_routes.rs",
        handler: "get_session_input_projection",
    },
    GatewayRouteBinding {
        route: routes::GET_API_SESSIONS_BY_ID_INPUTS,
        source: "message_routes.rs",
        handler: "get_session_input_projection",
    },
    GatewayRouteBinding {
        route: routes::GET_API_SESSIONS_BY_ID_LIFECYCLE,
        source: "session_routes.rs",
        handler: "session_lifecycle_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_SESSIONS_BY_ID_MESSAGES,
        source: "message_routes.rs",
        handler: "get_session_messages",
    },
    GatewayRouteBinding {
        route: routes::GET_API_SESSIONS_BY_ID_MISSION_FOCUS,
        source: "session_routes.rs",
        handler: "get_mission_focus_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_SESSIONS_BY_ID_REPLAY,
        source: "session_routes.rs",
        handler: "replay_session_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_SESSIONS_BY_ID_STATS,
        source: "session_routes.rs",
        handler: "get_session_stats_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_SESSIONS_BY_ID_TASK_FOCUS,
        source: "session_routes.rs",
        handler: "get_task_focus_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_SESSIONS_BY_ID_TURN_INBOX,
        source: "message_routes.rs",
        handler: "get_turn_inbox",
    },
    GatewayRouteBinding {
        route: routes::GET_API_SESSIONS_BY_ID_TURNS_BY_TURN_ID_EVIDENCE,
        source: "session_routes.rs",
        handler: "get_turn_evidence",
    },
    GatewayRouteBinding {
        route: routes::GET_API_SESSIONS_BY_ID_TURNS_BY_TURN_ID_INBOX,
        source: "message_routes.rs",
        handler: "get_turn_inbox_by_path",
    },
    GatewayRouteBinding {
        route: routes::GET_API_SESSIONS_EXECUTION_POLICY_DEFAULTS,
        source: "session_routes.rs",
        handler: "get_execution_policy_defaults",
    },
    GatewayRouteBinding {
        route: routes::GET_API_SESSIONS_EXECUTIONS,
        source: "session_routes.rs",
        handler: "list_running_session_execution_indices",
    },
    GatewayRouteBinding {
        route: routes::GET_API_SESSIONS_SEARCH,
        source: "session_routes.rs",
        handler: "search_messages_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_SKILLS_BY_ID,
        source: "skill_routes.rs",
        handler: "skill_get_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_SKILLS_BY_ID_ACTIVE_POINTER,
        source: "skill_routes.rs",
        handler: "skill_active_pointer_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_SKILLS_BY_ID_FILES,
        source: "skill_routes.rs",
        handler: "skill_files_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_SKILLS_BY_ID_FILES_RAW,
        source: "skill_routes.rs",
        handler: "skill_file_raw_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_SKILLS_CATALOG,
        source: "skill_routes.rs",
        handler: "skills_catalog_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_SKILLS_MAINTENANCE,
        source: "skill_routes.rs",
        handler: "skill_maintenance_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_SKILLS_MAINTENANCE_BY_ID,
        source: "skill_routes.rs",
        handler: "skill_maintenance_detail_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_SKILLS_PROJECTION,
        source: "skill_routes.rs",
        handler: "skills_projection_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_SKILLS_REVISION_REVIEWS_BY_ID,
        source: "skill_routes.rs",
        handler: "skill_revision_review_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_SKILLS_RUNS,
        source: "skill_routes.rs",
        handler: "skill_runs_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_SKILLS_RUNS_BY_ID,
        source: "skill_routes.rs",
        handler: "skill_run_detail_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_SLASH,
        source: "slash_routes.rs",
        handler: "slash_catalog_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_SLASH_BY_ID,
        source: "slash_routes.rs",
        handler: "slash_detail_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_SLASH_HISTORY,
        source: "slash_routes.rs",
        handler: "slash_history_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_SURFACES,
        source: "surface_routes.rs",
        handler: "list_surfaces_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_SURFACES_BY_ID,
        source: "surface_routes.rs",
        handler: "get_surface_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_SURFACES_BY_ID_DELIVERIES,
        source: "surface_routes.rs",
        handler: "get_surface_deliveries_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_SURFACES_BY_ID_EVENTS,
        source: "surface_routes.rs",
        handler: "get_surface_events_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_SURFACES_BY_ID_HEALTH,
        source: "surface_routes.rs",
        handler: "get_surface_health_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_SURFACES_BY_ID_INBOX,
        source: "surface_routes.rs",
        handler: "get_surface_inbox_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_SURFACES_BY_ID_MESSAGES,
        source: "surface_routes.rs",
        handler: "get_surface_messages_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_SURFACES_BY_ID_OUTBOX,
        source: "surface_routes.rs",
        handler: "get_surface_outbox_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_SURFACES_BY_ID_OUTBOX_BY_DELIVERY_ID,
        source: "surface_routes.rs",
        handler: "get_surface_outbox_delivery_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_SURFACES_BY_ID_RESOURCES,
        source: "surface_routes.rs",
        handler: "get_surface_resources_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_SURFACES_BY_ID_ROUTES,
        source: "surface_routes.rs",
        handler: "get_surface_routes_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_SURFACES_BY_ID_STATUS,
        source: "surface_routes.rs",
        handler: "get_surface_status_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_SURFACES_BY_ID_TRIGGER_EVENTS,
        source: "surface_routes.rs",
        handler: "get_surface_trigger_events_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_SURFACES_HEALTH,
        source: "surface_routes.rs",
        handler: "surface_health_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_TASKS,
        source: "task_routes.rs",
        handler: "tasks_status_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_TASKS_BY_ID,
        source: "task_routes.rs",
        handler: "task_detail_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_TASKS_BY_ID_EXECUTION_GRAPH,
        source: "agent_routes.rs",
        handler: "task_execution_graph_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_TASKS_BY_ID_TURNS,
        source: "task_routes.rs",
        handler: "task_turns_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_TASKS_MISSION_ORGANIZATION,
        source: "task_routes.rs",
        handler: "mission_organization_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_TEAM_TEMPLATES,
        source: "agent_routes.rs",
        handler: "team_templates_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_TOOLS,
        source: "system_routes.rs",
        handler: "tools_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_TOOLS_CACHE,
        source: "system_routes.rs",
        handler: "tool_cache_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_TOOLS_CHECKPOINTS,
        source: "system_routes.rs",
        handler: "tool_checkpoints_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_TOOLS_CHECKPOINTS_BY_ID_DIFF,
        source: "system_routes.rs",
        handler: "tool_checkpoint_diff_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_USAGE,
        source: "system_routes.rs",
        handler: "usage_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_WEBUI_MANIFEST,
        source: "public_routes.rs",
        handler: "webui_manifest_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_WORKSPACE,
        source: "workspace_routes.rs",
        handler: "workspace_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_WORKSPACE_DOWNLOAD,
        source: "workspace_routes.rs",
        handler: "download_workspace_path_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_WORKSPACE_FILES,
        source: "workspace_routes.rs",
        handler: "workspace_files_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_WORKSPACE_META,
        source: "workspace_routes.rs",
        handler: "workspace_meta_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_API_WORKSPACES,
        source: "workspace_routes.rs",
        handler: "workspaces_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_APPS_BY_APP_ID,
        source: "app_routes.rs",
        handler: "serve_index",
    },
    GatewayRouteBinding {
        route: routes::GET_APPS_BY_APP_ID_WILDCARD_PATH,
        source: "app_routes.rs",
        handler: "serve_asset",
    },
    GatewayRouteBinding {
        route: routes::GET_HEALTH,
        source: "public_routes.rs",
        handler: "health_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_HEALTHZ,
        source: "public_routes.rs",
        handler: "gateway_health_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_READYZ,
        source: "public_routes.rs",
        handler: "gateway_ready_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_S_BY_SURFACE_WILDCARD_PATH,
        source: "surface_routes.rs",
        handler: "surface_static_handler",
    },
    GatewayRouteBinding {
        route: routes::GET_SURFACE_CALLBACK_BY_SURFACE_WILDCARD_PATH,
        source: "surface_routes.rs",
        handler: "surface_callback_handler",
    },
    GatewayRouteBinding {
        route: routes::PATCH_API_MEMORY_ENTRY_BY_ID,
        source: "memory_routes.rs",
        handler: "update_memory_entry_handler",
    },
    GatewayRouteBinding {
        route: routes::PATCH_API_MEMORY_MAINTENANCE_BY_ID,
        source: "memory_routes.rs",
        handler: "update_memory_maintenance_handler",
    },
    GatewayRouteBinding {
        route: routes::PATCH_API_MISSION_SCHEDULES_BY_ID,
        source: "mission_routes.rs",
        handler: "update_mission_schedule_handler",
    },
    GatewayRouteBinding {
        route: routes::PATCH_API_RUNTIME_LIVE_SUBSCRIPTIONS_BY_ID,
        source: "live_routes.rs",
        handler: "patch_live_subscription",
    },
    GatewayRouteBinding {
        route: routes::PATCH_API_SESSIONS_BY_ID,
        source: "session_routes.rs",
        handler: "update_session_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_AGENTS_ASSEMBLE,
        source: "agent_routes.rs",
        handler: "agent_assemble_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_APPROVAL_GRANTS_BY_ID_REVOKE,
        source: "approval_routes.rs",
        handler: "approval_grant_revoke_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_APPROVAL_PRUNE,
        source: "approval_routes.rs",
        handler: "approval_prune_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_APPROVAL_RESPOND,
        source: "approval_routes.rs",
        handler: "approval_respond_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_APPROVAL_RISK_RECEIPT,
        source: "approval_routes.rs",
        handler: "risk_receipt_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_APPS_BY_APP_ID_OPERATIONS_BY_OPERATION_ID_INVOKE,
        source: "app_routes.rs",
        handler: "invoke_operation",
    },
    GatewayRouteBinding {
        route: routes::POST_API_APPS_BY_APP_ID_OPERATIONS_BY_OPERATION_ID_STREAM,
        source: "app_routes.rs",
        handler: "stream_operation",
    },
    GatewayRouteBinding {
        route: routes::POST_API_APPS_BY_APP_ID_RESTART,
        source: "app_routes.rs",
        handler: "restart_app",
    },
    GatewayRouteBinding {
        route: routes::POST_API_APPS_BY_APP_ID_SUBSCRIPTIONS_BY_SUBSCRIPTION_ID_ACK,
        source: "app_routes.rs",
        handler: "ack_subscription",
    },
    GatewayRouteBinding {
        route: routes::POST_API_APPS_BY_APP_ID_TUI_VIEWS_BY_VIEW_ID_ACTIONS,
        source: "app_routes.rs",
        handler: "tui_action",
    },
    GatewayRouteBinding {
        route: routes::POST_API_APPS_BY_APP_ID_TUI_VIEWS_BY_VIEW_ID_OPEN,
        source: "app_routes.rs",
        handler: "tui_open",
    },
    GatewayRouteBinding {
        route: routes::POST_API_APPS_BY_APP_ID_TUI_VIEWS_BY_VIEW_ID_STREAM,
        source: "app_routes.rs",
        handler: "tui_stream",
    },
    GatewayRouteBinding {
        route: routes::POST_API_AUTH_LOGIN,
        source: "public_routes.rs",
        handler: "login_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_AUTH_LOGOUT,
        source: "public_routes.rs",
        handler: "logout_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_CONNECTORS_RESOURCES_PROMOTE_MEMORY,
        source: "connector_routes.rs",
        handler: "connector_resource_promote_memory_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_CONNECTORS_RESOURCES_REVALIDATE,
        source: "connector_routes.rs",
        handler: "connector_resource_revalidate_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_CONNECTORS_SERVICES_BY_SERVICE_ID_EXECUTE,
        source: "connector_routes.rs",
        handler: "connector_service_execute_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_CONNECTORS_SOURCES_BY_ADAPTER_ID_POLL_EVENTS,
        source: "connector_routes.rs",
        handler: "connector_source_poll_events_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_CONNECTORS_SOURCES_BY_ADAPTER_ID_RUN_INCREMENTAL,
        source: "connector_routes.rs",
        handler: "connector_source_run_incremental_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_COWD_STRUCTURED_INGEST_PLAN,
        source: "core_routes.rs",
        handler: "structured_ingest_plan_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_CROSS_PLANE_ACTION_EXECUTE,
        source: "cross_plane_routes.rs",
        handler: "cross_plane_action_execute_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_CROSS_PLANE_ACTION_PREFLIGHT,
        source: "cross_plane_routes.rs",
        handler: "cross_plane_action_preflight_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_CROSS_PLANE_GRANTS,
        source: "cross_plane_routes.rs",
        handler: "cross_plane_create_grant_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_CROSS_PLANE_IDENTITIES,
        source: "cross_plane_routes.rs",
        handler: "cross_plane_create_identity_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_CROSS_PLANE_IDENTITY_RESOLVE,
        source: "cross_plane_routes.rs",
        handler: "cross_plane_identity_resolve_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_CROSS_PLANE_POLICY_SIMULATE,
        source: "cross_plane_routes.rs",
        handler: "cross_plane_policy_simulate_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_EDGES_RELOAD,
        source: "edge_routes.rs",
        handler: "edge_reload_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_EVIDENCE_RESOLVE_BATCH,
        source: "context_routes.rs",
        handler: "resolve_evidence_refs_batch_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_EVOLUTION_CANDIDATES,
        source: "evolution_routes.rs",
        handler: "evolution_candidate_create_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_EVOLUTION_CANDIDATES_BY_ID_EVALUATE,
        source: "evolution_routes.rs",
        handler: "evolution_candidate_evaluate_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_EVOLUTION_CANDIDATES_BY_ID_REVIEWS_CANARY,
        source: "evolution_routes.rs",
        handler: "evolution_candidate_canary_review_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_EVOLUTION_CANDIDATES_BY_ID_REVIEWS_STABLE,
        source: "evolution_routes.rs",
        handler: "evolution_candidate_stable_review_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_EVOLUTION_CASES_BY_ID_ANALYZE,
        source: "evolution_routes.rs",
        handler: "evolution_case_analyze_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_EVOLUTION_DIAGNOSES,
        source: "evolution_routes.rs",
        handler: "evolution_diagnosis_create_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_EVOLUTION_EVALUATION_POLICY_REVIEWS,
        source: "evolution_routes.rs",
        handler: "evolution_evaluation_policy_change_request_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_EVOLUTION_EVALUATION_POLICY_REVIEWS_BY_ID_DECISION,
        source: "evolution_routes.rs",
        handler: "evolution_evaluation_policy_decision_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_EVOLUTION_PROPOSALS,
        source: "evolution_routes.rs",
        handler: "evolution_proposal_create_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_EVOLUTION_PROPOSALS_BY_ID_DECISION,
        source: "evolution_routes.rs",
        handler: "evolution_proposal_decision_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_EVOLUTION_REVIEWS,
        source: "evolution_routes.rs",
        handler: "evolution_release_change_request_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_EVOLUTION_REVIEWS_BY_ID_DECISION,
        source: "evolution_routes.rs",
        handler: "evolution_review_decision_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_EVOLUTION_SIGNALS,
        source: "evolution_routes.rs",
        handler: "evolution_signal_create_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_HARNESS_EVAL_RUNS,
        source: "harness_eval_routes.rs",
        handler: "harness_eval_start_run_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_HARNESS_EVAL_RUNS_BY_ID_CANCEL,
        source: "harness_eval_routes.rs",
        handler: "harness_eval_cancel_run_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_MATRIX_COMPUTE_JOBS_BY_ID_RUN,
        source: "matrix_routes.rs",
        handler: "matrix_compute_job_run_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_MATRIX_COMPUTE_JOBS_PLAN,
        source: "matrix_routes.rs",
        handler: "matrix_compute_job_plan_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_MATRIX_DATA_PLANE_INGEST_PLAN,
        source: "matrix_routes.rs",
        handler: "matrix_data_plane_ingest_plan_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_MATRIX_ENTITIES_CONFLICT_DECISION,
        source: "matrix_routes.rs",
        handler: "matrix_entity_conflict_decision_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_MATRIX_ENTITIES_MATCH_CANDIDATE,
        source: "matrix_routes.rs",
        handler: "matrix_entity_match_candidate_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_MATRIX_ENTITIES_RESOLVE_SOURCE_KEY,
        source: "matrix_routes.rs",
        handler: "matrix_entity_resolve_source_key_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_MATRIX_ENTITIES_UPSERT,
        source: "matrix_routes.rs",
        handler: "matrix_entity_upsert_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_MATRIX_EVIDENCE_BY_ID_QUALITY_GATE,
        source: "matrix_routes.rs",
        handler: "matrix_evidence_quality_gate_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_MATRIX_EVIDENCE_BUILD,
        source: "matrix_routes.rs",
        handler: "matrix_evidence_build_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_MATRIX_FACTS_INGEST,
        source: "matrix_routes.rs",
        handler: "matrix_fact_ingest_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_MATRIX_METRIC_DEPENDENCIES_AFFECTED_BY_FACT_TYPE,
        source: "matrix_routes.rs",
        handler: "matrix_metric_affected_by_fact_type_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_MATRIX_METRIC_DEPENDENCIES_UPSERT,
        source: "matrix_routes.rs",
        handler: "matrix_metric_dependency_upsert_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_MATRIX_METRICS_ATTENTION_PLAN,
        source: "matrix_routes.rs",
        handler: "matrix_metric_attention_plan_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_MATRIX_METRICS_RECOMPUTE,
        source: "matrix_routes.rs",
        handler: "matrix_metric_recompute_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_MATRIX_METRICS_SNAPSHOTS_MATERIALIZE,
        source: "matrix_routes.rs",
        handler: "matrix_metric_snapshot_materialize_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_MATRIX_RELATIONS_UPSERT,
        source: "matrix_routes.rs",
        handler: "matrix_relation_upsert_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_MATRIX_SOURCE_PACKS_BY_ID_CONNECTOR_RUNS_PLAN,
        source: "matrix_routes.rs",
        handler: "matrix_source_pack_connector_run_plan_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_MATRIX_SOURCE_PACKS_BY_ID_CONNECTOR_RUNS_RUN,
        source: "matrix_routes.rs",
        handler: "matrix_source_pack_connector_run_execute_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_MATRIX_SOURCE_PACKS_BY_ID_DELTA_PLAN,
        source: "matrix_routes.rs",
        handler: "matrix_source_pack_delta_plan_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_MATRIX_SOURCE_PACKS_BY_ID_INGEST_FILE,
        source: "matrix_routes.rs",
        handler: "matrix_source_pack_ingest_file_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_MATRIX_SOURCE_PACKS_BY_ID_SNAPSHOTS_PLAN,
        source: "matrix_routes.rs",
        handler: "matrix_source_snapshot_plan_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_MATRIX_SOURCE_PACKS_BY_ID_SNAPSHOTS_RUN,
        source: "matrix_routes.rs",
        handler: "matrix_source_snapshot_run_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_MATRIX_SOURCE_PACKS_BY_ID_VALIDATE,
        source: "matrix_routes.rs",
        handler: "matrix_source_pack_validate_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_MATRIX_SOURCE_PACKS_UPSERT,
        source: "matrix_routes.rs",
        handler: "matrix_source_pack_upsert_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_MEMORY_BY_LAYER,
        source: "memory_routes.rs",
        handler: "create_memory_entry_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_MEMORY_KNOWLEDGE_CANDIDATES_BY_ID_ROLLBACK,
        source: "memory_routes.rs",
        handler: "rollback_memory_knowledge_candidate_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_MEMORY_MAINTENANCE,
        source: "memory_routes.rs",
        handler: "scan_memory_maintenance_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_MEMORY_SYMBOL_LINKS,
        source: "memory_routes.rs",
        handler: "create_memory_symbol_link_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_MESSAGE_CONNECTORS_BY_NAME_REPAIR,
        source: "message_connector_routes.rs",
        handler: "repair_message_connector_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_MESSAGE_CONNECTORS_WECHAT_ILINK_ACTIONS_ACCOUNT_LOGIN_QR_POLL,
        source: "message_connector_routes.rs",
        handler: "wechat_ilink_qr_poll_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_MESSAGE_CONNECTORS_WECHAT_ILINK_ACTIONS_ACCOUNT_LOGIN_QR_START,
        source: "message_connector_routes.rs",
        handler: "wechat_ilink_qr_start_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_MISSION_APPROVALS,
        source: "mission_routes.rs",
        handler: "submit_mission_approval_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_MISSION_APPROVALS_BY_ID_DECISION,
        source: "mission_routes.rs",
        handler: "decide_mission_approval_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_MISSION_CONTROL,
        source: "mission_routes.rs",
        handler: "execute_mission_control_command_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_MISSION_CONTROL_INTERPRET,
        source: "mission_routes.rs",
        handler: "interpret_mission_command_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_MISSION_CONTROL_SESSIONS_BRIDGE,
        source: "mission_routes.rs",
        handler: "bridge_mission_session_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_MISSION_CONTROL_TEAMS_BY_TEAM_ID_CANCEL,
        source: "mission_routes.rs",
        handler: "cancel_team_runtime_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_MISSION_PROXIES,
        source: "mission_routes.rs",
        handler: "upsert_mission_proxy_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_MISSION_SCHEDULES,
        source: "mission_routes.rs",
        handler: "create_mission_schedule_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_MISSION_SCHEDULES_BY_ID_PAUSE,
        source: "mission_routes.rs",
        handler: "pause_mission_schedule_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_MISSION_SCHEDULES_BY_ID_RESUME,
        source: "mission_routes.rs",
        handler: "resume_mission_schedule_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_MISSION_SCHEDULES_BY_ID_RUN,
        source: "mission_routes.rs",
        handler: "run_mission_schedule_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_MISSION_SCHEDULES_TICK,
        source: "mission_routes.rs",
        handler: "tick_mission_schedules_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_MISSION_SESSIONS,
        source: "mission_routes.rs",
        handler: "start_mission_session_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_MISSION_SESSIONS_BY_ID_BACKGROUND,
        source: "mission_routes.rs",
        handler: "background_mission_session_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_MISSION_SESSIONS_BY_ID_CLOSE,
        source: "mission_routes.rs",
        handler: "close_mission_session_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_MISSION_SESSIONS_BY_ID_PAUSE,
        source: "mission_routes.rs",
        handler: "pause_mission_session_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_MISSION_SESSIONS_BY_ID_SWITCH,
        source: "mission_routes.rs",
        handler: "switch_mission_session_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_PROFILES,
        source: "profile_routes.rs",
        handler: "create_profile_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_PROFILES_SWITCH,
        source: "profile_routes.rs",
        handler: "switch_profile_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_RESOURCES,
        source: "resource_routes.rs",
        handler: "upload_resource_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_RUNTIME_AGENTS_BY_ID_CANCEL,
        source: "agent_routes.rs",
        handler: "runtime_agent_cancel_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_RUNTIME_AGENTS_BY_ID_INPUT,
        source: "agent_routes.rs",
        handler: "runtime_agent_input_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_RUNTIME_AGENTS_BY_ID_INTERRUPT,
        source: "agent_routes.rs",
        handler: "runtime_agent_interrupt_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_RUNTIME_AGENTS_BY_ID_SHUTDOWN,
        source: "agent_routes.rs",
        handler: "runtime_agent_shutdown_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_RUNTIME_CONFIG_RELOAD,
        source: "runtime_routes.rs",
        handler: "reload_runtime_config",
    },
    GatewayRouteBinding {
        route: routes::POST_API_RUNTIME_EVENTS_RECOVER,
        source: "runtime_routes.rs",
        handler: "recover_runtime_events",
    },
    GatewayRouteBinding {
        route: routes::POST_API_RUNTIME_EXECUTIONS_BY_ID_COMMANDS,
        source: "route_registry.rs",
        handler: "execute_projection_command",
    },
    GatewayRouteBinding {
        route: routes::POST_API_RUNTIME_LIVE_SUBSCRIPTIONS,
        source: "live_routes.rs",
        handler: "create_live_subscription",
    },
    GatewayRouteBinding {
        route: routes::POST_API_RUNTIME_MANAGED_AGENTS_BY_ID_HEALTH_RESET,
        source: "managed_agent_routes.rs",
        handler: "managed_agent_health_reset_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_RUNTIME_MANAGED_AGENTS_BY_ID_TRIGGER,
        source: "managed_agent_routes.rs",
        handler: "managed_agent_manual_trigger_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_RUNTIME_MANAGED_AGENTS_DEFINITIONS,
        source: "managed_agent_routes.rs",
        handler: "managed_agent_definition_create_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_RUNTIME_MANAGED_AGENTS_DISPATCH,
        source: "managed_agent_routes.rs",
        handler: "managed_agent_dispatch_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_RUNTIME_MANAGED_AGENTS_EVENTS,
        source: "managed_agent_routes.rs",
        handler: "managed_agent_event_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_RUNTIME_OUTBOX_BY_DIRECTION_BY_ID_RETRY,
        source: "runtime_routes.rs",
        handler: "retry_runtime_outbox",
    },
    GatewayRouteBinding {
        route: routes::POST_API_RUNTIME_PROVIDERS_RELOAD,
        source: "runtime_routes.rs",
        handler: "reload_runtime_providers",
    },
    GatewayRouteBinding {
        route: routes::POST_API_RUNTIME_SESSION_LEASES_ACQUIRE,
        source: "runtime_routes.rs",
        handler: "acquire_runtime_session_lease",
    },
    GatewayRouteBinding {
        route: routes::POST_API_RUNTIME_SESSION_LEASES_RELEASE,
        source: "runtime_routes.rs",
        handler: "release_runtime_session_lease",
    },
    GatewayRouteBinding {
        route: routes::POST_API_RUNTIME_TURNS,
        source: "runtime_routes.rs",
        handler: "submit_runtime_turn",
    },
    GatewayRouteBinding {
        route: routes::POST_API_RUNTIME_TURNS_BY_ID_CANCEL,
        source: "runtime_routes.rs",
        handler: "cancel_runtime_turn",
    },
    GatewayRouteBinding {
        route: routes::POST_API_RUNTIME_UPGRADE_DISPOSITIONS,
        source: "runtime_routes.rs",
        handler: "record_upgrade_disposition",
    },
    GatewayRouteBinding {
        route: routes::POST_API_RUNTIME_UPGRADE_EXPORT,
        source: "runtime_routes.rs",
        handler: "export_upgrade_manifest",
    },
    GatewayRouteBinding {
        route: routes::POST_API_RUNTIME_UPGRADE_MAINTENANCE,
        source: "runtime_routes.rs",
        handler: "enter_upgrade_maintenance",
    },
    GatewayRouteBinding {
        route: routes::POST_API_SESSIONS,
        source: "session_routes.rs",
        handler: "create_session",
    },
    GatewayRouteBinding {
        route: routes::POST_API_SESSIONS_BY_ID_ARCHIVE,
        source: "session_routes.rs",
        handler: "archive_session_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_SESSIONS_BY_ID_ATTACH,
        source: "session_routes.rs",
        handler: "attach_session_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_SESSIONS_BY_ID_ATTACHMENTS,
        source: "workspace_routes.rs",
        handler: "add_session_attachment_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_SESSIONS_BY_ID_BRANCH,
        source: "session_routes.rs",
        handler: "branch_session_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_SESSIONS_BY_ID_CANCEL,
        source: "session_routes.rs",
        handler: "cancel_session_turn_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_SESSIONS_BY_ID_COMPACT,
        source: "session_routes.rs",
        handler: "compact_session_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_SESSIONS_BY_ID_CONTEXT_RECOMMENDATIONS,
        source: "context_routes.rs",
        handler: "record_context_recommendation_action",
    },
    GatewayRouteBinding {
        route: routes::POST_API_SESSIONS_BY_ID_DETACH,
        source: "session_routes.rs",
        handler: "detach_session_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_SESSIONS_BY_ID_ENSURE,
        source: "session_routes.rs",
        handler: "ensure_surface_session_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_SESSIONS_BY_ID_FINALIZE,
        source: "session_routes.rs",
        handler: "finalize_session_turn_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_SESSIONS_BY_ID_INPUTS_BY_INPUT_ID_CANCEL,
        source: "message_routes.rs",
        handler: "cancel_session_input",
    },
    GatewayRouteBinding {
        route: routes::POST_API_SESSIONS_BY_ID_INPUTS_BY_INPUT_ID_RECLASSIFY,
        source: "message_routes.rs",
        handler: "reclassify_session_input",
    },
    GatewayRouteBinding {
        route: routes::POST_API_SESSIONS_BY_ID_MESSAGES,
        source: "message_routes.rs",
        handler: "send_message",
    },
    GatewayRouteBinding {
        route: routes::POST_API_SKILLS,
        source: "skill_routes.rs",
        handler: "skill_create_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_SKILLS_BY_ID_ACTIONS_PLAN,
        source: "skill_routes.rs",
        handler: "skill_action_plan_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_SKILLS_BY_ID_ACTIONS_RUN,
        source: "skill_routes.rs",
        handler: "skill_action_run_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_SKILLS_BY_ID_ACTIONS_VALIDATE,
        source: "skill_routes.rs",
        handler: "skill_action_validate_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_SKILLS_BY_ID_ROLLBACK_REVIEWS,
        source: "skill_routes.rs",
        handler: "skill_revision_rollback_review_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_SKILLS_BY_ID_TRANSLATE,
        source: "skill_routes.rs",
        handler: "skill_translate_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_SKILLS_INSTALL_COMMIT,
        source: "skill_routes.rs",
        handler: "skill_install_commit_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_SKILLS_INSTALL_PLAN,
        source: "skill_routes.rs",
        handler: "skill_install_plan_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_SKILLS_INSTALL_UPLOAD_COMMIT,
        source: "skill_routes.rs",
        handler: "skill_upload_commit_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_SKILLS_INSTALL_UPLOAD_PLAN,
        source: "skill_routes.rs",
        handler: "skill_upload_plan_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_SKILLS_MAINTENANCE_BY_ID_ACTIVATION_REVIEWS,
        source: "skill_routes.rs",
        handler: "skill_revision_activation_review_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_SKILLS_REVISION_REVIEWS_BY_ID_DECISION,
        source: "skill_routes.rs",
        handler: "skill_revision_review_decision_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_SLASH_DISPATCH,
        source: "slash_routes.rs",
        handler: "slash_dispatch_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_SLASH_RESOLVE,
        source: "slash_routes.rs",
        handler: "slash_resolve_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_SURFACES_BY_ID_ACTION,
        source: "surface_routes.rs",
        handler: "action_surface_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_SURFACES_BY_ID_HEALTH_CHECK,
        source: "surface_routes.rs",
        handler: "post_surface_health_check_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_SURFACES_BY_ID_INBOX_BY_MESSAGE_ID_REPLAY,
        source: "surface_routes.rs",
        handler: "replay_surface_inbox_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_SURFACES_BY_ID_MESSAGES_ARCHIVE,
        source: "surface_routes.rs",
        handler: "archive_surface_messages_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_SURFACES_BY_ID_MESSAGES_PURGE_ARCHIVED_EVENTS,
        source: "surface_routes.rs",
        handler: "purge_archived_surface_messages_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_SURFACES_BY_ID_OUTBOX_BY_DELIVERY_ID_DEAD_LETTER,
        source: "surface_routes.rs",
        handler: "dead_letter_surface_outbox_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_SURFACES_BY_ID_OUTBOX_BY_DELIVERY_ID_RETRY,
        source: "surface_routes.rs",
        handler: "retry_surface_outbox_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_SURFACES_BY_ID_REPAIR,
        source: "surface_routes.rs",
        handler: "repair_surface_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_SURFACES_BY_ID_RESTART,
        source: "surface_routes.rs",
        handler: "restart_surface_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_SURFACES_BY_ID_SEND,
        source: "surface_routes.rs",
        handler: "send_surface_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_SURFACES_BY_ID_START,
        source: "surface_routes.rs",
        handler: "start_surface_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_SURFACES_BY_ID_STOP,
        source: "surface_routes.rs",
        handler: "stop_surface_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_SURFACES_BY_ID_TRIGGER_EVENTS_RETRY,
        source: "surface_routes.rs",
        handler: "retry_surface_trigger_event_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_TASKS_BY_ID_CANCEL,
        source: "task_routes.rs",
        handler: "cancel_task_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_TASKS_BY_ID_COMPLETE,
        source: "task_routes.rs",
        handler: "complete_task_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_TASKS_BY_ID_FAILURE,
        source: "task_routes.rs",
        handler: "record_task_failure_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_TASKS_BY_ID_FOCUS,
        source: "task_routes.rs",
        handler: "focus_task_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_TASKS_BY_ID_MISSION_COMMIT,
        source: "task_routes.rs",
        handler: "commit_task_mission_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_TASKS_BY_ID_MISSION_PREVIEW,
        source: "task_routes.rs",
        handler: "preview_task_mission_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_TASKS_BY_ID_PHASES,
        source: "task_routes.rs",
        handler: "start_task_phase_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_TASKS_BY_ID_PHASES_BY_PHASE_ID_ARTIFACTS,
        source: "task_routes.rs",
        handler: "record_task_phase_artifact_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_TASKS_BY_ID_PHASES_BY_PHASE_ID_REVIEW,
        source: "task_routes.rs",
        handler: "review_task_phase_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_TASKS_MISSION_COMMIT,
        source: "task_routes.rs",
        handler: "commit_tasks_mission_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_TASKS_MISSION_PREVIEW,
        source: "task_routes.rs",
        handler: "preview_tasks_mission_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_TASKS_START,
        source: "task_routes.rs",
        handler: "start_task_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_TEAM_TEMPLATES_INSTANTIATE,
        source: "agent_routes.rs",
        handler: "team_template_instantiate_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_TOOLS_BATCH_READONLY,
        source: "system_routes.rs",
        handler: "tool_batch_readonly_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_TOOLS_CHECKPOINTS,
        source: "system_routes.rs",
        handler: "tool_checkpoint_create_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_TOOLS_CHECKPOINTS_BY_ID_RESTORE,
        source: "system_routes.rs",
        handler: "tool_checkpoint_restore_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_TOOLS_CONTEXT_FANOUT_PLAN,
        source: "system_routes.rs",
        handler: "tool_context_fanout_plan_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_TOOLS_EXECUTE,
        source: "system_routes.rs",
        handler: "tool_execute_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_TOOLS_INTENT_PLAN,
        source: "system_routes.rs",
        handler: "tool_intent_plan_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_TOOLS_MUTATIONS_APPLY,
        source: "system_routes.rs",
        handler: "tool_mutation_apply_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_TOOLS_MUTATIONS_PREVIEW,
        source: "system_routes.rs",
        handler: "tool_mutation_preview_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_UPLOAD,
        source: "workspace_routes.rs",
        handler: "upload_workspace_file_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_WORKSPACE_DIRS,
        source: "workspace_routes.rs",
        handler: "create_workspace_dir_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_WORKSPACE_FILES,
        source: "workspace_routes.rs",
        handler: "create_workspace_file_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_API_WORKSPACE_RENAME,
        source: "workspace_routes.rs",
        handler: "rename_workspace_path_handler",
    },
    GatewayRouteBinding {
        route: routes::POST_SURFACE_CALLBACK_BY_SURFACE_WILDCARD_PATH,
        source: "surface_routes.rs",
        handler: "surface_callback_handler",
    },
    GatewayRouteBinding {
        route: routes::PUT_API_APPROVAL_CONFIG,
        source: "approval_routes.rs",
        handler: "update_approval_config_handler",
    },
    GatewayRouteBinding {
        route: routes::PUT_API_CONFIG,
        source: "system_routes.rs",
        handler: "update_config_handler",
    },
    GatewayRouteBinding {
        route: routes::PUT_API_SESSIONS_BY_ID_EXECUTION_POLICY,
        source: "session_routes.rs",
        handler: "put_session_execution_policy",
    },
    GatewayRouteBinding {
        route: routes::PUT_API_SESSIONS_BY_ID_MISSION_FOCUS,
        source: "session_routes.rs",
        handler: "set_mission_focus_handler",
    },
    GatewayRouteBinding {
        route: routes::PUT_API_SESSIONS_BY_ID_TASK_FOCUS,
        source: "session_routes.rs",
        handler: "set_task_focus_handler",
    },
    GatewayRouteBinding {
        route: routes::PUT_API_SESSIONS_EXECUTION_POLICY_DEFAULTS,
        source: "session_routes.rs",
        handler: "put_execution_policy_defaults",
    },
];

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn every_surface_route_has_exactly_one_handler_binding() {
        let bound = GATEWAY_ROUTE_BINDINGS
            .iter()
            .map(|binding| {
                assert!(!binding.source.is_empty());
                assert!(!binding.handler.is_empty());
                (
                    binding.route.method().as_str(),
                    binding.route.path().template(),
                )
            })
            .collect::<BTreeSet<_>>();
        let catalog = surface::gateway_api::gateway_routes()
            .iter()
            .map(|route| (route.method().as_str(), route.path().template()))
            .collect::<BTreeSet<_>>();
        assert_eq!(GATEWAY_ROUTE_BINDINGS.len(), bound.len());
        assert_eq!(bound, catalog);
    }
}
