# Graph Report - C:\Work\Work\Personal\Ray_Complete_Project\ray  (2026-09-05)

## Corpus Check
- Large corpus: 318 files · ~526,936 words. Semantic extraction will be expensive (many Claude tokens). Consider running on a subfolder.

## Summary
- 2038 nodes · 2831 edges · 183 communities (174 shown, 9 thin omitted)
- Extraction: 95% EXTRACTED · 5% INFERRED · 0% AMBIGUOUS · INFERRED: 132 edges (avg confidence: 0.66)
- Token cost: unavailable — host subagent tools did not expose token usage; zero placeholders are not a cost measurement.

## Community Hubs (Navigation)
- Governed Cloud Operations
- Pipeline Migration and Git
- Report Design Principles
- Ontology and Schema Deployment
- Dedicated Pool Lakehouse Assessment
- Pipeline Migration Verification
- Databricks and HDInsight Migration
- Report Layout Planning
- Ray Architecture and Operations
- Eventhouse KQL Operations
- Report Themes and Visuals
- Ray CLI and Runtime
- Telegram Gateway Handling
- Ontology Query Routing
- Migration and Dataflow Upgrades
- Activator Data Sources
- Project Configuration and Schemas
- Fabric Core CLI Patterns
- Variable Library Lifecycle
- Definition Diff and Tokens
- Local Demo and Memory
- Task Control and Recovery
- Task Service Integration
- Cloud Action Regression Coverage
- Orchestration Recovery Coverage
- Ontology Definition Authoring
- Theme Text Styles
- Fabric Cost Estimation
- Fabric Transport Isolation
- Hive and Lake Database Migration
- DAX Performance Analysis
- Chart Theme Properties
- Eventstream Topology Operations
- Dataflow Execution Templates
- Dataflow Definition Lifecycle
- Warehouse SQL Features
- Warehouse Query Workflows
- SQL Performance Diagnostics
- Orchestration Context and Validation
- SQL Database Authoring
- Cartesian Visual Formatting
- Synapse Connection Migration
- Theme Colors and Status
- Card and Textbox Styles
- Activator Actions and Validation
- Warehouse Authoring Templates
- Notebook Lakehouse Operations
- Persistent Task State
- Spark Consumption Routing
- Synapse Runtime Compatibility
- Item Definitions and Activator
- Dataflow Preview and M
- SQL Database Core Operations
- SQL Database Discovery
- Dataflow Mode Dispatch
- Synapse Migration Capacity
- Monitor API and Identity
- Conditional Formatting Cascade
- Warehouse Operational Diagnostics
- Dedicated Pool Conversion Deployment
- Spark Lakehouse Operations
- SQL Database Diagnostics
- Report Pages and Maps
- Report Filters and Chrome
- Report Images and Backgrounds
- Synapse Notebook Migration
- Notebook Libraries and ML
- Semantic Model AI Readiness
- Base Report Theme
- Azure Monitor Fabric Onboarding
- Dataflow Query Exploration
- Medallion Data Architecture
- FabricIQ Report Queries
- Spark Performance Patterns
- Report Deployment and Binding
- Spark Notebook Authoring
- Semantic Model Connection Binding
- DAX Language Patterns
- Report Interaction Design
- Materialized View Operations
- Semantic Modeling Guidelines
- Semantic Model REST APIs
- Visual Formatting Recipes
- Synapse Fabric Feature Parity
- Spark Pool Environment Migration
- Migration Security and Governance
- Comparative Dashboard Design
- Operational Dashboard Design
- Warehouse SQL Authoring
- Activator Rule Conditions
- Inventory Domain Conventions
- Spark Failure Diagnosis
- Spark Event Log Retrieval
- Pipeline Notebook Diagnostics
- Report Query Expressions
- Semantic Model Metadata Discovery
- Semantic Model Naming
- PBIP Model File Structure
- Telemetry Source Tables
- TMDL Model Authoring
- Report Typography
- Inventory Transformation Tests
- Spark Data Engineering
- Fabric Infrastructure Orchestration
- Materialized View Design
- Synapse Migration Reporting
- Synapse Migration Orchestration
- Materialized View Incremental Refresh
- Notebook Definition and DAGs
- Synapse Utility API Migration
- Migration Validation and Cutover
- Semantic Model Authoring
- Analytical Dashboard Design
- Executive Dashboard Design
- Dataflow Connection Management
- Dataflow Output Destinations
- Ontology Design Review
- Notebook Context Resolution
- Spark Monitoring APIs
- Fabric Data Access Patterns
- Spark Automated Diagnostics
- Spark Diagnostic Tiers
- Livy Session Health
- Power BI Report Authoring
- Monitoring Business Analysis
- Power BI KPI Cards
- Power BI Chart Colors
- Direct Lake Modeling
- Spark Diagnostic Operations
- Power BI Desktop Verification
- Synapse Code Modernization
- Notebook Development Workflow
- Fabric Catalog Search
- Dataflow Authoring Bar Charts
- Dataflow Consumption Bar Charts
- Power BI Slicer Theme
- Dataflow Source Connectors
- Eventstream Consumption
- Dataflow Inventory Queries
- Warehouse Metadata Discovery
- Ray Evaluation Criteria
- Notebook Runtime Parameters
- Notebook Troubleshooting Logs
- Spark History Analysis
- Ray Incident Investigation
- Ray Persona Boundaries
- Application Insights Correlation
- Activator Rule Inspection
- Real Time Dashboards
- Eventhouse Delta Shortcuts
- Mirrored Catalog Operations
- Monitoring Operations Agent
- Power BI Screenshot Review
- PBIR Version Control
- Windows Dependency Lockfile
- Databricks Code Migration
- Databricks Migration Compatibility
- Ray Implementation Guidance
- Dataflow Authoring CLI
- Notebook External Connections
- Notebook Shared Resources
- Codex SDK Worker
- Log Analytics OAuth
- Power BI Authoring CLI
- Fabric Workspace Discovery
- Sample Fabric Configuration
- Eventhouse Bash Ingestion
- Eventhouse Bash Schema Deployment
- Eventhouse Policy Configuration
- Fabric CLI Profile Isolation
- Dataflow Authoring Line Charts
- Dataflow Consumption Line Charts
- Eventhouse Schema Export
- Ray Engineering Package
- Ray Project Metadata

## God Nodes (most connected - your core abstractions)
1. `Control` - 51 edges
2. `CloudActions` - 35 edges
3. `StateStore` - 32 edges
4. `Gateway` - 27 edges
5. `main()` - 25 edges
6. `Orchestrator` - 24 edges
7. `FabricGateway` - 22 edges
8. `Power BI PBIR report authoring` - 19 edges
9. `Azure Monitor Fabric Onboarding` - 18 edges
10. `Semantic model authoring` - 18 edges

## Surprising Connections (you probably didn't know these)
- `test_shared_service_updates_then_runs_job()` --calls--> `TaskService`  [INFERRED]
  tests/test_cloud.py → src/ray_de/service.py
- `FakeRunner` --uses--> `ProjectConfig`  [INFERRED]
  tests/test_recovery.py → src/ray_de/config.py
- `FakeRunner` --uses--> `Project`  [INFERRED]
  tests/test_recovery.py → src/ray_de/config.py
- `test_duplicate_yaml_key_rejected()` --calls--> `load_project()`  [EXTRACTED]
  tests/test_policy.py → src/ray_de/config.py
- `API` --uses--> `TaskStopped`  [INFERRED]
  tests/test_telegram.py → src/ray_de/control.py

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **Complete Dataflow Gen2 definition parts** — common_dataflows_authoring_core_querymetadata_json, common_dataflows_authoring_core_mashup_pq, common_dataflows_authoring_core_dataflow_platform_metadata [EXTRACTED 1.00]
- **Dataflow output destination contract** — authoring_output_destinations_datadestinations_annotation, authoring_output_destinations_hidden_dataflow_destination_query, authoring_output_destinations_destination_query_metadata [EXTRACTED 1.00]
- **Medallion data refinement flow** — e2e_medallion_architecture_skill_bronze_medallion_layer, e2e_medallion_architecture_skill_silver_medallion_layer, e2e_medallion_architecture_skill_gold_medallion_layer [EXTRACTED 1.00]
- **Ontology graph binding model** — authoring_ontology_authoring_core_ontology_entitytype, authoring_ontology_authoring_core_ontology_relationshiptype, authoring_ontology_authoring_core_ontology_databinding, authoring_ontology_authoring_core_ontology_contextualization [EXTRACTED 1.00]
- **Notebook connection and library prerequisites** — resources_pipeline_orchestrator_pipeline_migration_orchestration, resources_linked_service_to_connection_pipeline_linked_service_connection_migration, resources_notebook_activity_migration_pipeline_notebook_activity_conversion, resources_pipeline_orchestrator_variable_library_attachment_gate [EXTRACTED 1.00]
- **Mechanical report geometry contract** — references_design_brief_mechanical_layout_contract, references_design_brief_layout_grid_regions, references_design_brief_visual_semantic_placements, references_design_brief_layout_space_audit [EXTRACTED 1.00]
- **PBIR rendered verification loop** — references_powerbi_report_author_cli_pbir_validation, references_screenshot_review_rendered_output_verification, references_screenshot_review_data_rendering_audit [EXTRACTED 1.00]
- **Fabric event logs to local Spark History Server** — operations_jobinsight_api_logutils_copyeventlog, operations_jobinsight_api_onelake_event_log_staging, operations_jobinsight_api_onelake_event_log_download, operations_spark_history_server_spark_event_log_reconstruction [EXTRACTED 1.00]
- **SQLDB slowness investigation** — references_operations_sqldb_performance_investigation, operations_query_reference_sqldb_coefficient_of_variation, operations_query_reference_sqldb_wait_category_analysis, references_operations_sqldb_blocking_investigation [EXTRACTED 1.00]
- **Dedicated Pool artifact migration** — resources_dedicated_pool_conversion_dedicated_pool_spark_sql_conversion_rules, resources_dedicated_pool_conversion_dedicated_pool_deterministic_source_block_audit, resources_dedicated_pool_deployment_dedicated_pool_generated_notebook_publication, resources_dedicated_pool_deployment_dedicated_pool_artifact_readiness_gate [EXTRACTED 1.00]

## Communities (183 total, 9 thin omitted)

### Community 0 - "Governed Cloud Operations"
Cohesion: 0.11
Nodes (23): CloudActions, definition_parts(), digest(), FabricTransport, The service explicitly reported a terminal failure., RemoteFailed, response_body(), authorize() (+15 more)

### Community 1 - "Pipeline Migration and Git"
Cohesion: 0.05
Nodes (49): commitToGit, ConfiguredConnection, Fabric Git Integration Operations, updateFromGit, workspaceHead, workspaceRelations, Dataset Inlining, Fabric Connections (+41 more)

### Community 2 - "Report Design Principles"
Cohesion: 0.04
Nodes (47): Annotation layer order, Narrative 7 5 Split variant, Narrative Annotated Hero variant, Narrative Data Story, Narrative Martini glass, Narrative Single Column variant, Segel and Heer story patterns, Canonical design contract (+39 more)

### Community 3 - "Ontology and Schema Deployment"
Cohesion: 0.05
Nodes (42): Ontology Contextualization, Ontology DataBinding, Ontology EntityType, Ontology ID generation, Ontology RelationshipType, Ontology schema reference, Ontology static first bindings, Deployment pipeline item pairing (+34 more)

### Community 4 - "Dedicated Pool Lakehouse Assessment"
Cohesion: 0.06
Nodes (40): blindSpots, complexity-report.json, DacFx, DACPAC, Dedicated Pool discovery and assessment, migration-gap-report.json, schema-inventory.json, T1-T4 (+32 more)

### Community 5 - "Pipeline Migration Verification"
Cohesion: 0.06
Nodes (40): Fabric connection creation contract, Fabric connection name references, Pipeline linked service connection migration, SHIR gateway replacement, WorkspaceIdentity credential preference, Disjoint migration report categories, Incremental migration records, Pipeline migration reporting (+32 more)

### Community 6 - "Databricks and HDInsight Migration"
Cohesion: 0.05
Nodes (39): Databricks Fabric Migration, dbutils, Delta Live Tables, Fabric Environments, notebookutils, Spark Job Definition, Unity Catalog, Fabric Environment (+31 more)

### Community 7 - "Report Layout Planning"
Cohesion: 0.05
Nodes (39): Design Brief, layout_contract, Power BI report planning, powerbi-report-authoring, powerbi-report-design, Publish, report-spec.md, space_audit (+31 more)

### Community 8 - "Ray Architecture and Operations"
Cohesion: 0.06
Nodes (38): Host owned policy boundary, Isolated official Codex SDK worker, Pinned vendored reference skills, Ray architecture, Shared task service, Source bound action plan, SQLite tasks and audit, Cloud setup and action flow (+30 more)

### Community 9 - "Eventhouse KQL Operations"
Cohesion: 0.05
Nodes (38): Eventhouse CLI skill, Eventhouse KQL authoring, Eventhouse mode dispatch, Eventhouse readonly KQL, Eventhouse terminal write verification, Eventhouse advanced operations, Eventhouse authoring monitoring, Eventhouse functions and update policies (+30 more)

### Community 10 - "Report Themes and Visuals"
Cohesion: 0.06
Nodes (36): Design Brief contract, Design contract provenance marker, Header and slicer band, Layout grid regions, Layout space audit, Mechanical layout contract, Visual semantic placements, Atomic theme and override sweep (+28 more)

### Community 11 - "Ray CLI and Runtime"
Cohesion: 0.13
Nodes (22): Governed cloud boundary, doctor(), emit(), main(), parser(), Source-bound Fabric actions. This is the only write executor in Ray., CodexRunner, Run the official SDK in a short-lived worker; parent owns durable state. (+14 more)

### Community 12 - "Telegram Gateway Handling"
Cohesion: 0.12
Nodes (17): chunks(), Gateway, main(), API, Service, setup(), test_allowlist_private_only_and_deduplication(), test_callback_survives_restart_and_cannot_replay() (+9 more)

### Community 13 - "Ontology Query Routing"
Cohesion: 0.08
Nodes (32): Cross-source relationship traversal, eventhouse-cli, grounding JSON, Ontology Consumption Examples, sqldw-cli, Anchor, Direction, grounding JSON (+24 more)

### Community 14 - "Migration and Dataflow Upgrades"
Cohesion: 0.06
Nodes (32): Dataflow dependency rebinding, Dataflow Gen1 upgrade workflow, Gen1 readiness classification, Gen2 upgrade generation boundary, saveAsNativeArtifact, Databricks notebookutils API mapping, Fabric runtime context, Notebookutils credentials (+24 more)

### Community 15 - "Activator Data Sources"
Cohesion: 0.06
Nodes (31): Digital Twin Builder source, Ontology, query.compositeKey, Snapshot Mode, Time-axis mode, Activator destination, Activator Eventstream Source, eventstreamSource-v1 (+23 more)

### Community 16 - "Project Configuration and Schemas"
Cohesion: 0.11
Nodes (17): BaseModel, FabricConfig, load_project(), Policy, Project, ProjectConfig, StrictModel, unique_mapping() (+9 more)

### Community 17 - "Fabric Core CLI Patterns"
Cohesion: 0.06
Nodes (31): Azure CLI az rest, Continuation pagination, Fabric CLI implementation patterns, Fabric skill attribution header, Long running operation polling, OneLake curl access, SQL TDS sqlcmd access, Workspace and item discovery (+23 more)

### Community 18 - "Variable Library Lifecycle"
Cohesion: 0.08
Nodes (30): Variable Library authoring reference, Variable Library ConnectionReference, Variable Library Definition plane, Variable Library Item-state plane, Variable Library ItemReference, Variable Library settings.json, Variable Library variableOverrides, Variable Library variables.json (+22 more)

### Community 19 - "Definition Diff and Tokens"
Cohesion: 0.10
Nodes (24): Exception, Token, decode_text(), diff_definitions(), diff_part(), extract_parts(), InputError, json_diff() (+16 more)

### Community 20 - "Local Demo and Memory"
Cohesion: 0.17
Nodes (20): fixture(), main(), Offline demonstration: real host logic, synthetic model and Fabric responses. N, run_demo(), SyntheticFabric, SyntheticRunner, export(), Local operational evidence and user usefulness ratings, without invented scores. (+12 more)

### Community 21 - "Task Control and Recovery"
Cohesion: 0.10
Nodes (8): Control, Service shutdown interrupts workers while preserving pending user decisions., test_cancel_kills_validation_process(), test_decisions_are_scoped_one_shot(), test_memory_rejects_secret(), test_monitor_quiet_until_change_or_failure(), test_service_shutdown_preserves_decisions_but_interrupts_workers(), test_stop_generation_survives_restart_and_resume()

### Community 22 - "Task Service Integration"
Cohesion: 0.18
Nodes (14): UniqueLoader, Durable channel, decision, approval and interruption state., TaskStopped, Read-only, opt-in single polling cycle; scheduler ownership stays with the user., One host workflow shared by the terminal and Telegram channel., TaskService, project_lock(), OS lock releases on process death, unlike a persistent 'busy' flag. (+6 more)

### Community 23 - "Cloud Action Regression Coverage"
Cohesion: 0.17
Nodes (20): definition(), prepare(), test_definition_path_rejected(), test_dev_update_replay_and_receipt(), test_job_failure_never_claims_success_or_retries(), test_job_requires_matching_reviewed_definition(), test_lost_write_response_reconciles_without_resubmit(), test_new_review_does_not_revalidate_old_plan() (+12 more)

### Community 24 - "Orchestration Recovery Coverage"
Cohesion: 0.26
Nodes (14): Orchestrator, TurnResult, FakeRunner, output(), test_clarification_requires_question_and_recommendation(), test_completion_requires_evidence(), test_failed_validation_prevents_review_and_completion(), test_recovery_pauses_execution_preserves_clarification() (+6 more)

### Community 25 - "Ontology Definition Authoring"
Cohesion: 0.13
Nodes (18): Contextualization, entityIdParts, InlineBase64, NonTimeSeries, Ontology Authoring Mechanics, TimeSeries, updateDefinition, getDefinition (+10 more)

### Community 26 - "Theme Text Styles"
Cohesion: 0.12
Nodes (17): color, fontFace, fontSize, color, fontFace, fontSize, color, fontFace (+9 more)

### Community 27 - "Fabric Cost Estimation"
Cohesion: 0.12
Nodes (17): Autoscale Billing for Spark, Concurrent demand sizing, Fabric Cost Estimation, Fabric sizing pilot validation, Live regional Fabric pricing, Unified Fabric capacity billing, Current platform spend, Fabric cost estimation worksheet (+9 more)

### Community 28 - "Fabric Transport Isolation"
Cohesion: 0.21
Nodes (11): main(), NoRedirect, Pinned Fabric CLI authentication with exactly one host-controlled HTTP request., request(), Opener, Response, test_auth_adapter_uses_scope_list_without_interactive_renew(), test_post_error_not_retried_or_payload_exposed() (+3 more)

### Community 29 - "Hive and Lake Database Migration"
Cohesion: 0.14
Nodes (17): DBS, External Hive Metastore migration, JDBC, MSCK REPAIR TABLE, OneLake Shortcuts, SDS, Separate Lakehouses, TBLS (+9 more)

### Community 30 - "DAX Performance Analysis"
Cohesion: 0.15
Nodes (16): CallbackDataID, DAX performance decision guide, Formula Engine, SE Parallelism Factor, SE Query Fusion, semantic equivalence, Storage Engine, xmSQL (+8 more)

### Community 31 - "Chart Theme Properties"
Cohesion: 0.29
Nodes (15): *, categoryAxis, *, *, *, dataPoint, labels, legend (+7 more)

### Community 32 - "Eventstream Topology Operations"
Cohesion: 0.14
Nodes (15): Eventstream CLI skill, Eventstream mode dispatch, Eventstream persisted topology verification, Eventstream read write boundary, Eventstream authoring reference, Eventstream derived stream, Eventstream filter operator, Eventstream full topology deployment (+7 more)

### Community 33 - "Dataflow Execution Templates"
Cohesion: 0.15
Nodes (14): Dataflow authoring script templates, Dataflow binding readback, Dataflow preview save loop, Dataflow smoke test, Fabric asynchronous result retrieval, Dataflow consumption CLI quick reference, Dataflow definition decoding, Dataflow job monitoring (+6 more)

### Community 34 - "Dataflow Definition Lifecycle"
Cohesion: 0.18
Nodes (14): Dataflow definition, Dataflow Execute jobs, Dataflow Git lifecycle, Dataflow platform metadata, Fabric Dataflows Gen2 authoring, Fast Copy engine, mashup.pq, queryMetadata.json (+6 more)

### Community 35 - "Warehouse SQL Features"
Cohesion: 0.14
Nodes (14): SQL database temporal tables, SQL warehouse authoring reference, Warehouse COPY INTO, Warehouse CTAS, Warehouse schema evolution, Warehouse snapshot isolation, Warehouse time travel, SQL endpoint granular security (+6 more)

### Community 36 - "Warehouse Query Workflows"
Cohesion: 0.15
Nodes (14): SQLDW consumption quick reference, SQLDW multiple result handling, SQLDW paginated result handling, SQLDW standalone query scripts, SQLDW consumption workflow templates, SQLDW CSV data export, SQLDW legacy TDS fallback, SQLDW performance investigation template (+6 more)

### Community 37 - "SQL Performance Diagnostics"
Cohesion: 0.15
Nodes (14): SQLDB coefficient of variation, SQLDB database scoped Extended Events, SQLDB operations query catalog, SQLDB Query Store, SQLDB regressed query detection, SQLDB resource usage DMV, SQLDB wait category analysis, SQLDW baseline regression comparison (+6 more)

### Community 38 - "Orchestration Context and Validation"
Cohesion: 0.26
Nodes (10): changed(), load_context(), manifest(), repo_digest(), validate(), run_checked(), terminate_tree(), Path (+2 more)

### Community 39 - "SQL Database Authoring"
Cohesion: 0.15
Nodes (13): Fabric SQL Database authoring, Go sqlcmd authoring, SQL authoring verification flow, SQL Database endpoint discovery, SQL Database enforced constraints, SQL schema discovery, SqlPackage schema deployment, Existing SQL Database resolution (+5 more)

### Community 40 - "Cartesian Visual Formatting"
Cohesion: 0.17
Nodes (13): Cartesian Category and Y roles, Cartesian category drill hierarchy, Cartesian logarithmic axis constraint, Cartesian metadata series selectors, Column and bar chart families, Line chart secondary axis, Power BI cartesian visual authoring, Navigator dual entry pattern (+5 more)

### Community 41 - "Synapse Connection Migration"
Cohesion: 0.17
Nodes (13): Direct Key Vault migration, External database Fabric connection, Linked Service replacement mapping, OneLake shortcut storage migration, Self hosted IR gateway migration, Synapse connectivity migration, ADLS OAuth provider migration, Cosmos DB explicit credential migration (+5 more)

### Community 42 - "Theme Colors and Status"
Cohesion: 0.17
Nodes (12): *, bad, center, dataColors, dropShadow, good, maximum, minimum (+4 more)

### Community 43 - "Card and Textbox Styles"
Cohesion: 0.18
Nodes (12): background, border, cardCalloutArea, *, label, padding, spacing, * (+4 more)

### Community 44 - "Activator Actions and Validation"
Cohesion: 0.18
Nodes (12): Activator action types reference, Activator dynamic function parameters, EmailMessage, Fabric item action, FabricItemBinding, TeamsMessage, Activator AttributeTrigger, Activator authoring reference (+4 more)

### Community 45 - "Warehouse Authoring Templates"
Cohesion: 0.17
Nodes (12): SQLDW authoring workflow templates, SQLDW COPY INTO template, SQLDW CTAS migration template, SQLDW historical recovery template, SQLDW incremental upsert retry, SQLDW staged ELT template, SQLDW authoring reference, SQLDW DDL constraints (+4 more)

### Community 46 - "Notebook Lakehouse Operations"
Cohesion: 0.18
Nodes (12): Data discovery approach, Notebook code generation approach, Notebook code generation rules, Notebook module index, Spark notebook authoring reference, ABFSS paths, Default lakehouse mount, Lakehouse path construction reference (+4 more)

### Community 48 - "Spark Consumption Routing"
Cohesion: 0.17
Nodes (12): Fabric Spark Livy consumption, Idle Livy session reuse, Lakehouse Livy sessions, Livy session configuration, Livy statements, SQL endpoint query routing, Fabric MLV operations mode, Fabric Spark authoring mode (+4 more)

### Community 49 - "Synapse Runtime Compatibility"
Cohesion: 0.18
Nodes (12): azure-cosmos-analytics-spark, environment.yml, Fabric Runtime 1.3, GPU, Synapse Fabric library compatibility, xgboost, DELTA_PROTOCOL_MISMATCH, GPU_POOL_UNSUPPORTED (+4 more)

### Community 50 - "Item Definitions and Activator"
Cohesion: 0.18
Nodes (11): Activator CLI skill, Activator mode dispatcher, Activator source validation gate, Activator terminal write verification, ReflexEntities definition, Definition envelope, Fabric item definition structures, Platform metadata file (+3 more)

### Community 51 - "Dataflow Preview and M"
Cohesion: 0.20
Nodes (11): Dataflow M language semantics, M each scoping, M optional field access, M per cell conversion errors, M try error records, Bounded Dataflow preview, Custom mashup section document, Dataflow executeQuery (+3 more)

### Community 52 - "SQL Database Core Operations"
Cohesion: 0.18
Nodes (11): SQL database auditing configuration, SQL database authoring reference, SQL database automatic mirroring, SQL database lifecycle, SQL database SqlPackage, SQL database vector columns, SQL database consumption reference, SQL database consumption security (+3 more)

### Community 53 - "SQL Database Discovery"
Cohesion: 0.18
Nodes (11): SQLDB catalog exploration, SQLDB cross database discovery, SQLDB extended discovery queries, SQLDB security discovery, SQLDB table health checks, SQLDB consumption reference, SQLDB mirroring security gap, SQLDB OLTP endpoint (+3 more)

### Community 54 - "Dataflow Mode Dispatch"
Cohesion: 0.18
Nodes (11): Dataflow authoring mode, Dataflow consumption mode, Dataflow terminal persistence, Dataflow upgrade mode, Fabric Dataflow CLI dispatcher, Dataflow ApplyChangesIfNeeded, Dataflow connection bootstrap, Dataflow full replacement save (+3 more)

### Community 55 - "Synapse Migration Capacity"
Cohesion: 0.18
Nodes (11): Fabric Capacity Units, Synapse capacity sizing reference, Synapse Fabric cost model comparison, Synapse Fabric sizing factors, Synapse Spark to Fabric capacity planning, Dedicated Pool procedure notebook mapping, Dedicated Pool schema and code only scope, Synapse ordered migration phases (+3 more)

### Community 56 - "Monitor API and Identity"
Cohesion: 0.18
Nodes (11): Azure Monitor Fabric API, Mirrored Catalog, OAuth2, OperationsAgent, Service Principal, supportedConnectionTypes, Owner, provisionIdentity (+3 more)

### Community 57 - "Conditional Formatting Cascade"
Cohesion: 0.18
Nodes (11): Conditional Cases formatting, Field driven color, FillRule gradients, Power BI conditional formatting, Table column data bars, Table icon sets, Complete VCO override bundle, Formatting selector precedence (+3 more)

### Community 58 - "Warehouse Operational Diagnostics"
Cohesion: 0.18
Nodes (11): SQLDW cache warmth diagnostics, SQLDW clustering recommendations, SQLDW fresh evidence reporting, SQLDW operations reference, SQLDW pool pressure diagnostics, SQLDW Query Insights diagnostics, Lakehouse SQL endpoint item identity, SQLDW CLI skill (+3 more)

### Community 59 - "Dedicated Pool Conversion Deployment"
Cohesion: 0.20
Nodes (11): Dedicated Pool conversion complexity tiers, Dedicated Pool conversion reference, Dedicated Pool deployability gate, Dedicated Pool deterministic source block audit, Dedicated Pool notebook parameter contract, Dedicated Pool artifact readiness gate, Dedicated Pool deployment order, Dedicated Pool deployment reference (+3 more)

### Community 60 - "Spark Lakehouse Operations"
Cohesion: 0.20
Nodes (10): Compute pool strategy, Duplicate job submission prevention, Lakehouse management, Notebook management, Spark authoring reference, Cross lakehouse analytics, Lakehouse Livy sessions, OneLake Table APIs (+2 more)

### Community 61 - "SQL Database Diagnostics"
Cohesion: 0.20
Nodes (10): SQLDB diagnostic examples, SQLDB idle head blocker example, SQLDB index recommendation example, SQLDB intermittent slowness example, SQLDB automatic tuning priority, SQLDB blocking investigation, SQLDB bounded TDS fallback, SQLDB OLTP diagnostics (+2 more)

### Community 62 - "Report Pages and Maps"
Cohesion: 0.20
Nodes (10): PBIR drillthrough binding, PBIR identifier scopes, PBIR page registration, PBIR visual interactions, PBIR visual queryState projections, Power BI page and visual authoring workflows, Azure Map geocoding troubleshooting, Azure Map role bindings (+2 more)

### Community 63 - "Report Filters and Chrome"
Cohesion: 0.20
Nodes (10): Filter pane explicit theme styling, filterCard Applied Available states, outspacePane chrome, Power BI filter pane appearance, Filter Where source alias, PBIR filter scopes, Power BI filter definition authoring, Relative date time filter (+2 more)

### Community 64 - "Report Images and Backgrounds"
Cohesion: 0.22
Nodes (10): Chart plot area background image, Image content and container styling, ImageUrl model data category, Power BI image visual authoring, Public HTTPS image URL, RegisteredResources image binding, Page canvas background, Page outspace wallpaper (+2 more)

### Community 65 - "Synapse Notebook Migration"
Cohesion: 0.22
Nodes (10): shortcutMappings, InlineBase64, metadata.dependencies.lakehouse, Notebook, poolMappings, Scala/Java, SparkJobDefinitionV1.json, SparkJobDefinitionV2 (+2 more)

### Community 66 - "Notebook Libraries and ML"
Cohesion: 0.22
Nodes (9): Environment library publishing, Fabric built in runtime libraries, Notebook library management reference, Notebook uploaded libraries, MLflow autologging, MLflow experiment tracking, Notebook ML workflow reference, PREDICT scoring (+1 more)

### Community 67 - "Semantic Model AI Readiness"
Cohesion: 0.31
Nodes (9): AI Data Schema, AI instructions, Copilot, explicit measures, Prep data for AI, Semantic model AI readiness guidelines, TOM metadata, Verified Answers (+1 more)

### Community 68 - "Base Report Theme"
Cohesion: 0.32
Nodes (8): columnHeaders, *, *, values, visualStyles, lineChart, pivotTable, tableEx

### Community 69 - "Azure Monitor Fabric Onboarding"
Cohesion: 0.25
Nodes (8): Azure Monitor Fabric Onboarding, Business Insight Capture, External Delta table registration, Mirrored Catalog, OAuth, Operations Agent, Service Principal, Workspace identity

### Community 70 - "Dataflow Query Exploration"
Cohesion: 0.25
Nodes (8): ASCII Dataflow category chart, ASCII Dataflow line chart, Dataflow preview visualization, Dataflow Arrow sample rendering, Dataflow refresh history inspection, Fabric Dataflow consumption workflow, Read only Dataflow consumption, Saved and ad hoc Dataflow queries

### Community 71 - "Medallion Data Architecture"
Cohesion: 0.25
Nodes (8): Bronze medallion layer, Direct Lake semantic model, End-to-end medallion architecture, Gold medallion layer, Materialized Lake Views, Medallion pipeline orchestration, Schema enabled lakehouse, Silver medallion layer

### Community 72 - "FabricIQ Report Queries"
Cohesion: 0.29
Nodes (8): CustomInstructions, DiscoverArtifacts, ExecuteQuery, FabricIQ Power BI Consumption, GetReportMetadata, GetSemanticModelSchema, ValueSearch, Verified Answers

### Community 73 - "Spark Performance Patterns"
Cohesion: 0.25
Nodes (8): Fabric Spark performance patterns, Repeated DataFrame recomputation, Small Delta files, Spark capacity contention, Spark data skew, Spark garbage collection pressure, Spark shuffle spill, Spark task scheduling overhead

### Community 74 - "Report Deployment and Binding"
Cohesion: 0.25
Nodes (8): byConnection, byPath, LRO, PBIR, Power BI report management, rebind, semanticModelId, updateDefinition

### Community 75 - "Spark Notebook Authoring"
Cohesion: 0.25
Nodes (8): Fabric connection credentials, Fabric Spark notebook authoring, Notebook definition completion, Notebook lakehouse binding, Notebook run deduplication, Notebook runtime context, RunNotebook Jobs API, Spark session initialization

### Community 76 - "Semantic Model Connection Binding"
Cohesion: 0.32
Nodes (8): Automatic, bindConnection, connectionDetails, connectivityType, List Item Connections, None, refresh, Semantic model connection binding

### Community 77 - "DAX Language Patterns"
Cohesion: 0.25
Nodes (8): DEFINE, DIVIDE, Expr, GROUPBY, KEEPFILTERS, Semantic model DAX language guidelines, SUMMARIZECOLUMNS, TREATAS

### Community 78 - "Report Interaction Design"
Cohesion: 0.25
Nodes (8): Aggregation & Granularity, Annotation & Tooltip, drillthrough, Navigation, Power BI report design interactivity, Provenance & Personalization, Selection & Filtering, Shneiderman

### Community 79 - "Materialized View Operations"
Cohesion: 0.25
Nodes (8): Fabric MLV lifecycle operations, Lakehouse lineage schedules, Materialized view engine distinction, MLV execution definitions, MLV failure classification, MLV refresh job polling, MLV schedule replacement, MLV Spark discovery

### Community 80 - "Semantic Modeling Guidelines"
Cohesion: 0.25
Nodes (8): discourageImplicitMeasures, EntityPartitionSource, Explicit measures, MPartitionSource, Row-Level Security, Semantic model modeling guidelines, sortByColumn, Star schema

### Community 81 - "Semantic Model REST APIs"
Cohesion: 0.25
Nodes (8): API Audiences, executeQueries, Fabric Items API, Power BI Datasets API, Refresh, Semantic model REST API reference, TMDL, updateDefinition

### Community 82 - "Visual Formatting Recipes"
Cohesion: 0.25
Nodes (8): accentBar, cardVisual, FillRule, pivotTable, Power BI report design visual cookbook, selector, slicer, tableEx

### Community 83 - "Synapse Fabric Feature Parity"
Cohesion: 0.25
Nodes (8): Custom Pool, External Hive Metastore, Fabric Warehouse, notebookutils, spark.catalog, Starter Pool, Synapse Fabric feature parity reference, TokenLibrary

### Community 84 - "Spark Pool Environment Migration"
Cohesion: 0.32
Nodes (8): Fabric Environment, Custom Pool, environment.yml, Fabric Environment, Sparkcompute.yml, staging/publish, Starter Pool, Synapse Spark pool migration

### Community 85 - "Migration Security and Governance"
Cohesion: 0.32
Nodes (8): Azure Key Vault, Fabric Workspace Identity, Managed Private Endpoints, On-Premises Data Gateway, OneLake RBAC, RLS, Synapse migration security and governance, Workspace RBAC

### Community 86 - "Comparative Dashboard Design"
Cohesion: 0.29
Nodes (7): Absolute and relative variance, Comparative Benchmark, Comparative Side by Side variant, Comparative Slope Graph variant, Comparative Stacked Pairs variant, Comparison callout evidence, IBCS visual vocabulary

### Community 87 - "Operational Dashboard Design"
Cohesion: 0.29
Nodes (7): Four golden signals, Operational data freshness, Operational Four Up Status variant, Operational Incident First variant, Operational Monitor, Operational Wallboard variant, Semaphore state encoding

### Community 88 - "Warehouse SQL Authoring"
Cohesion: 0.29
Nodes (7): SQLDW authoring operation monitoring, SQLDW authoring quick reference, SQLDW bulk ingestion, SQLDW CTAS schema evolution, SQLDW MCP single batch, SQLDW time travel recovery, Dedicated Pool Spark SQL conversion rules

### Community 89 - "Activator Rule Conditions"
Cohesion: 0.29
Nodes (7): Activator Rule Conditions, AttributeTrigger, EventTrigger, ForNthTime, NumberBecomes, TeamsMessage, TimeDrivenWindowSpec

### Community 90 - "Inventory Domain Conventions"
Cohesion: 0.29
Nodes (7): Accepted fixture convention, Preserve product identifiers decision, String product identifiers, Bronze to Silver inventory transformation, Latest timestamp deduplication, Sample inventory project, String product identifiers

### Community 91 - "Spark Failure Diagnosis"
Cohesion: 0.29
Nodes (7): Driver memory failures, Executor memory failures, Fabric Spark job failure diagnosis, Shuffle fetch failures, Spark duration regression analysis, Spark monitoring log retrieval, Spark schema analysis errors

### Community 92 - "Spark Event Log Retrieval"
Cohesion: 0.29
Nodes (7): Fabric Spark JobInsight event-log API, JobInsight runtime prerequisites, JobInsight Scala runtime, LogUtils copyEventLog, OneLake event-log download, OneLake event-log staging, Spark attempt identification

### Community 93 - "Pipeline Notebook Diagnostics"
Cohesion: 0.29
Nodes (7): Combined pipeline severity report, Direct pipeline session correlation, Fabric pipeline notebook diagnosis, Failed-session Spark diagnosis, Notebook user-code failures, Pipeline activity edge cases, Pipeline activity-run evidence

### Community 94 - "Report Query Expressions"
Cohesion: 0.29
Nodes (7): PBIR Aggregation expressions, PBIR Column expressions, PBIR hierarchy expansion state, PBIR Measure expressions, PBIR sortDefinition, PBIR visual projection identity, Power BI semantic query expressions

### Community 95 - "Semantic Model Metadata Discovery"
Cohesion: 0.29
Nodes (7): INFO.DEPENDENCIES, INFO.ROLEMEMBERSHIPS, INFO.VIEW.COLUMNS, INFO.VIEW.MEASURES, INFO.VIEW.RELATIONSHIPS, INFO.VIEW.TABLES, Semantic model metadata discovery

### Community 96 - "Semantic Model Naming"
Cohesion: 0.29
Nodes (7): business language, Copilot, description, displayFolder, Measure Variations, Period Conventions, Semantic model naming conventions

### Community 97 - "PBIP Model File Structure"
Cohesion: 0.38
Nodes (7): byConnection, byPath, definition.pbir, definition.pbism, PBIR, Semantic model PBIP file reference, TMDL

### Community 98 - "Telemetry Source Tables"
Cohesion: 0.29
Nodes (7): AppDependencies, AppEvents, AppExceptions, AppRequests, Custom security tables, OpenTelemetry, Telemetry source tables

### Community 99 - "TMDL Model Authoring"
Cohesion: 0.29
Nodes (7): TMDL authoring guidelines, TMDL calculation groups, TMDL model references, TMDL relationships, TMDL row security, TMDL script commands, TMDL storage partitions

### Community 100 - "Report Typography"
Cohesion: 0.29
Nodes (7): callout, format, label, largeTitle, Power BI report design typography, Segoe UI, tabular

### Community 101 - "Inventory Transformation Tests"
Cohesion: 0.33
Nodes (3): Small inventory fixture for Ray local engineering evaluations., silver_inventory(), InventoryTests

### Community 102 - "Spark Data Engineering"
Cohesion: 0.29
Nodes (7): Broadcast dimension joins, Delta MERGE, Explicit ingestion schemas, Fabric Spark data engineering patterns, Medallion workload tuning, Remote Shuffle Manager, Window transformations

### Community 103 - "Fabric Infrastructure Orchestration"
Cohesion: 0.29
Nodes (7): Azure infrastructure deployment, Environment workspace isolation, Fabric artifact deployment, Fabric infrastructure and notebook orchestration, Hybrid infrastructure deployment, Lakehouse medallion schemas, Pipeline orchestration

### Community 104 - "Materialized View Design"
Cohesion: 0.29
Nodes (7): Deterministic MLV definitions, Fabric materialized lake view design patterns, Lakehouse MLV lineage, MLV row constraints, MLV topology consolidation, PySpark fmlv definitions, PySpark MLV full refresh

### Community 105 - "Synapse Migration Reporting"
Cohesion: 0.33
Nodes (7): SYNAPSESQL_NO_EQUIVALENT, Fabric Portal URL Patterns, FLAG_ANCHORS, migration_log, Synapse Fabric migration report, Synapse Source Links, warnings

### Community 106 - "Synapse Migration Orchestration"
Cohesion: 0.38
Nodes (7): lakehouseMappings, Lift-and-shift, Migrate-and-modernize, Migration State File, poolMappings, Synapse Fabric migration orchestrator, TokenManager

### Community 107 - "Materialized View Incremental Refresh"
Cohesion: 0.33
Nodes (7): Delta CDF append-only changes, Downstream presentation rewrites, Fabric MLV incremental refresh patterns, Incremental refresh blockers, Incremental SQL operators, MLV readiness assessment, SQL MLV incremental eligibility

### Community 108 - "Notebook Definition and DAGs"
Cohesion: 0.29
Nodes (7): Default lakehouse metadata, Fabric notebook API operations, InlineBase64 notebook payload, Jupyter notebook definition validation, Notebook DAG validation, Notebook definition update flow, Notebook runMultiple DAG

### Community 109 - "Synapse Utility API Migration"
Cohesion: 0.33
Nodes (7): mssparkutils, notebookutils, notebookutils.connection, notebookutils.credentials.getSecret, notebookutils.lakehouse, notebookutils.runtime, Synapse utility API mapping

### Community 110 - "Migration Validation and Cutover"
Cohesion: 0.29
Nodes (7): Cutover Readiness Criteria, Data Validation, Notebook Execution Testing, Query Result Comparison, SJD Execution Testing, Synapse environment validation, Synapse post-migration validation and testing

### Community 111 - "Semantic Model Authoring"
Cohesion: 0.29
Nodes (7): AI readiness, connection-binding.md, Direct Lake, Import, powerbi-modeling-mcp, Semantic model authoring, star schema

### Community 112 - "Analytical Dashboard Design"
Cohesion: 0.33
Nodes (6): Analytical Canvas, Analytical Filter Rail variant, Analytical Inline Slicers variant, Analytical Small Multiples Grid variant, Reproducible analytical cuts, Shneiderman mantra

### Community 113 - "Executive Dashboard Design"
Cohesion: 0.33
Nodes (6): Executive detail drillthrough, Executive Headline Hero variant, Executive Hero Right variant, Executive KPI Strip variant, Executive scan budget, Executive Summary

### Community 114 - "Dataflow Connection Management"
Cohesion: 0.33
Nodes (6): Dataflow composite connection identity, Dataflow connection management, Dataflow gateway routing, Dataflow multisource privacy firewall, Fabric connection capability discovery, Key Vault secret references

### Community 115 - "Dataflow Output Destinations"
Cohesion: 0.47
Nodes (6): DataDestinations annotation, Dataflow automatic replacement semantics, Dataflow draft reconciliation, Dataflow output destinations, Destination query metadata, Hidden Dataflow destination query

### Community 116 - "Ontology Design Review"
Cohesion: 0.33
Nodes (6): Affected parts, Brownfield, change set, Greenfield, Ontology Author-Time Design Review, updateDefinition

### Community 117 - "Notebook Context Resolution"
Cohesion: 0.33
Nodes (6): Notebook definition, Fabric context fallback, Lighter config, Local notebook context resolution, Notebook context resolution reference, Notebook dependency metadata

### Community 118 - "Spark Monitoring APIs"
Cohesion: 0.33
Nodes (6): Driver and executor log APIs, Livy Log API, Resource Usage API, Spark Advisor API, Spark History Server APIs, Spark monitoring reference

### Community 119 - "Fabric Data Access Patterns"
Cohesion: 0.33
Nodes (6): DefaultAzureCredential, KQL Database, Lakehouse SQL Endpoint, Microsoft Fabric Data Access Patterns, Ontology, pyodbc

### Community 120 - "Spark Automated Diagnostics"
Cohesion: 0.33
Nodes (6): Expired monitoring fallback, Fabric Spark automated diagnostic workflow, Spark diagnostic evidence ordering, Spark performance triage, Spark severity report, Spark state routing

### Community 121 - "Spark Diagnostic Tiers"
Cohesion: 0.33
Nodes (6): Fabric Spark diagnostic tiers, Offline Spark event-log tier, Online Spark monitoring tier, Spark diagnostic escalation triggers, Stage-focused task analysis, Two-tier Spark diagnosis

### Community 122 - "Livy Session Health"
Cohesion: 0.33
Nodes (6): Fabric Livy session health, Idle and zombie sessions, Livy resource monitoring, Livy session recovery, Livy session state machine, Stuck Livy startup

### Community 123 - "Power BI Report Authoring"
Cohesion: 0.33
Nodes (6): Modern Power BI visual types, Offline PBIR validation scope, PBIP report file layout, PBIR validate reload screenshot loop, Power BI authoring metadata CLI, Power BI PBIR report authoring

### Community 124 - "Monitoring Business Analysis"
Cohesion: 0.33
Nodes (6): Business analysis workflow, Correlation planning, dashboard, IncidentBin, Operations Agent, Playbook materialization

### Community 125 - "Power BI KPI Cards"
Cohesion: 0.40
Nodes (6): Card callout area formatting, Card clipping precheck, Card Data role binding, Card instance selectors, Multi value KPI card, Power BI cardVisual authoring

### Community 126 - "Power BI Chart Colors"
Cohesion: 0.40
Nodes (6): Measure color identity mapping, Non cartesian scopeId colors, Per measure Literal color selectors, Power BI chart color strategy, Single series defaultColor, Theme dataColors palette

### Community 127 - "Direct Lake Modeling"
Cohesion: 0.47
Nodes (6): AzureStorage.DataLake, Direct Lake modeling guidelines, EntityPartitionSource, expressionSource, OneLake, sourceColumn

### Community 128 - "Spark Diagnostic Operations"
Cohesion: 0.33
Nodes (6): Fabric Spark diagnostic operations, MLV diagnostic categories, Offline Spark History Server escalation, Read-only Spark diagnosis, Spark failure evidence priority, Spark monitoring API preference

### Community 129 - "Power BI Desktop Verification"
Cohesion: 0.33
Nodes (6): Desktop bridge PID selection, Desktop rendered output verification, Desktop theme filename cache, PBIP report reload boundary, Power BI Desktop verification workflow, Serial Desktop render operations

### Community 130 - "Synapse Code Modernization"
Cohesion: 0.33
Nodes (6): Synapse Fabric code patterns, Synapse notebook session migration, Synapse OneLake path migration, Synapse PolyBase COPY INTO migration, Synapse runtime context migration, Synapse Spark catalog substitutions

### Community 131 - "Notebook Development Workflow"
Cohesion: 0.33
Nodes (6): Fabric integration validation, Fabric notebook development workflow, Local Spark development tests, Notebook artifact deployment, Notebook debugging loop, Notebook lifecycle phases

### Community 132 - "Fabric Catalog Search"
Cohesion: 0.33
Nodes (6): Catalog.Read.All, continuationToken, displayName, Fabric Catalog Search consumption, filters, workspace

### Community 133 - "Dataflow Authoring Bar Charts"
Cohesion: 0.47
Nodes (5): bar_chart(), main(), pie_chart(), Horizontal bar chart -- works everywhere., Circular pie chart -- best with Unicode.

### Community 134 - "Dataflow Consumption Bar Charts"
Cohesion: 0.47
Nodes (5): bar_chart(), main(), pie_chart(), Horizontal bar chart -- works everywhere., Circular pie chart -- best with Unicode.

### Community 135 - "Power BI Slicer Theme"
Cohesion: 0.40
Nodes (5): date, header, items, *, slicer

### Community 136 - "Dataflow Source Connectors"
Cohesion: 0.40
Nodes (5): Dataflow connector binding model, Dataflow Power Query source connectors, Gateway free HTML parsing, Lakehouse M navigation, PowerPlatform Dataflows navigation

### Community 137 - "Eventstream Consumption"
Cohesion: 0.40
Nodes (5): Eventstream consumption reference, Eventstream definition, Eventstream downstream analytics, Eventstream health, Eventstream Topology API

### Community 138 - "Dataflow Inventory Queries"
Cohesion: 0.40
Nodes (5): Cross workspace Dataflow inventory, Dataflow discovery queries, Dataflow external connection analysis, Dataflow job history analysis, Dataflow parameter inspection

### Community 139 - "Warehouse Metadata Discovery"
Cohesion: 0.40
Nodes (5): SQLDW cross database discovery, SQLDW extended discovery queries, SQLDW schema and objects, SQLDW security discovery, SQLDW statistics metadata

### Community 140 - "Ray Evaluation Criteria"
Cohesion: 0.40
Nodes (5): Deterministic host validation, DEV and TEST acceptance gates, Human usefulness ratings, Ray evaluation, Twelve evaluation scenarios

### Community 141 - "Notebook Runtime Parameters"
Cohesion: 0.50
Nodes (5): Notebook configuration validation, Notebook parameterization, Notebook runtime context, Notebook runtime context reference, Notebook Variable Library

### Community 142 - "Notebook Troubleshooting Logs"
Cohesion: 0.40
Nodes (5): Blobfuse log, Notebook diagnosis approach, Notebook troubleshooting reference, Spark driver log, Spark executor log

### Community 143 - "Spark History Analysis"
Cohesion: 0.40
Nodes (5): Fabric event-log offline workflow, Local Spark History Server, Spark event-log reconstruction, Spark history analysis views, Spark history configuration

### Community 144 - "Ray Incident Investigation"
Cohesion: 0.40
Nodes (5): Evidence first diagnosis, Fabric incident investigation, Job reconciliation, Reviewed minimal local fix, Task events and action receipts

### Community 145 - "Ray Persona Boundaries"
Cohesion: 0.40
Nodes (5): Cloud action proposals, Evidence authority boundary, Ray, Ray senior data engineer persona, Structured completion evidence

### Community 146 - "Application Insights Correlation"
Cohesion: 0.40
Nodes (5): Application Insights dynamic fields, CustomDimensions, direct join, Properties, time-window correlation

### Community 147 - "Activator Rule Inspection"
Cohesion: 0.40
Nodes (5): Activator Consumption, get_activations_for_rule, getDefinition, powerBiSource-v1, ReflexEntities.json

### Community 148 - "Real Time Dashboards"
Cohesion: 0.40
Nodes (5): IncidentBins, KQLDashboard, queryRef, Real-Time Dashboard, RealTimeDashboard.json

### Community 149 - "Eventhouse Delta Shortcuts"
Cohesion: 0.40
Nodes (5): Delta log, Eventhouse OneLake shortcuts, external Delta table, query_acceleration, Tables/dbo

### Community 150 - "Mirrored Catalog Operations"
Cohesion: 0.40
Nodes (5): Discovery, Mirrored Catalog, Monitoring, Refresh, Selectable

### Community 151 - "Monitoring Operations Agent"
Cohesion: 0.40
Nodes (5): IncidentBins, KustoDatabase, Operations Agent, Playbook Generator, Teams notifications

### Community 152 - "Power BI Screenshot Review"
Cohesion: 0.40
Nodes (5): Data rendering audit, Layout visibility audit, Rendered output verification, Screenshot review, Theme consistency audit

### Community 153 - "PBIR Version Control"
Cohesion: 0.40
Nodes (5): PBIR edit branch, PBIR revert workflow, PBIR user commit gate, PBIR validation checkpoint, PBIR version control workflow

### Community 154 - "Windows Dependency Lockfile"
Cohesion: 0.40
Nodes (5): fabric-cicd 1.3.0, ms-fabric-cli 1.7.0, openai-codex 0.147.0, openai-codex-cli-bin 0.147.0, Windows Python 3.12 dependency snapshot

### Community 155 - "Databricks Code Migration"
Cohesion: 0.40
Nodes (5): Databricks to Fabric code patterns, Fabric Environment libraries, Fabric parameter cells, Notebookutils migration, OneLake path migration

### Community 156 - "Databricks Migration Compatibility"
Cohesion: 0.40
Nodes (5): Catalog to Lakehouse mapping, Databricks migration gotchas, Delta Live Tables rewrite, Photon to NEE compatibility, Structured Streaming trigger migration

### Community 157 - "Ray Implementation Guidance"
Cohesion: 0.50
Nodes (4): Evidence cannot authorize operations, Pinned official Codex SDK, Ray implementation guidance, Ray persona

### Community 158 - "Dataflow Authoring CLI"
Cohesion: 0.50
Nodes (4): Dataflow authoring CLI quick reference, Dataflow binary preview capture, Dataflow connection preflight, Dataflow operation and job status enums

### Community 159 - "Notebook External Connections"
Cohesion: 0.50
Nodes (4): Notebook Azure Key Vault secrets, Notebook Azure service tokens, Notebook connection credentials, Notebook external connections reference

### Community 160 - "Notebook Shared Resources"
Cohesion: 0.50
Nodes (4): Environment shared resources, Notebook builtin resources, Notebook resource absolute paths, Notebook resources reference

### Community 161 - "Codex SDK Worker"
Cohesion: 0.67
Nodes (3): atomic_json(), Private SDK subprocess. Inputs and state paths are supplied by the host., run()

### Community 162 - "Log Analytics OAuth"
Cohesion: 0.50
Nodes (4): Log Analytics workspace, Manage Connections, Mode B OAuth connection, OAuth

### Community 163 - "Power BI Authoring CLI"
Cohesion: 0.50
Nodes (4): Formatting capability discovery, PBIR validation, Power BI report author CLI reference, Visual catalog discovery

### Community 164 - "Fabric Workspace Discovery"
Cohesion: 0.50
Nodes (4): Azure Resource Manager, Fabric REST APIs, Fabric Workspace Discovery, user-provided workspace

### Community 165 - "Sample Fabric Configuration"
Cohesion: 0.50
Nodes (4): Local only write policy, Sample Fabric configuration, Sample Fabric project, Unittest source validation

### Community 169 - "Fabric CLI Profile Isolation"
Cohesion: 0.67
Nodes (3): Fabric CLI 1.7.0, Fabric CLI profile isolation, Project child process profile

## Ambiguous Edges - Review These
- `Fabric Capacity Units` → `Synapse Spark to Fabric capacity planning`  [AMBIGUOUS]
  src/ray_de/upstream/skills/synapse-migration/resources/capacity-sizing.md · relation: conceptually_related_to

## Knowledge Gaps
- **440 isolated node(s):** `ray-fabric-engineer`, `Path`, `export-schema.sh script`, `$schema`, `name` (+435 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **9 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **What is the exact relationship between `Fabric Capacity Units` and `Synapse Spark to Fabric capacity planning`?**
  _Edge tagged AMBIGUOUS (relation: conceptually_related_to) - confidence is low._
- **Why does `Offline demonstration` connect `Ray Architecture and Operations` to `Local Demo and Memory`?**
  _High betweenness centrality (0.009) - this node is a cross-community bridge._
- **Why does `Control` connect `Task Control and Recovery` to `Governed Cloud Operations`, `Orchestration Context and Validation`, `Ray CLI and Runtime`, `Telegram Gateway Handling`, `Project Configuration and Schemas`, `Definition Diff and Tokens`, `Local Demo and Memory`, `Task Service Integration`, `Cloud Action Regression Coverage`, `Orchestration Recovery Coverage`?**
  _High betweenness centrality (0.008) - this node is a cross-community bridge._
- **Are the 11 inferred relationships involving `Control` (e.g. with `CloudActions` and `FabricTransport`) actually correct?**
  _`Control` has 11 INFERRED edges - model-reasoned connections that need verification._
- **Are the 36 inferred relationships involving `ValueError` (e.g. with `main()` and `.get()`) actually correct?**
  _`ValueError` has 36 INFERRED edges - model-reasoned connections that need verification._
- **Are the 9 inferred relationships involving `CloudActions` (e.g. with `Control` and `PolicyError`) actually correct?**
  _`CloudActions` has 9 INFERRED edges - model-reasoned connections that need verification._
- **Are the 9 inferred relationships involving `StateStore` (e.g. with `SyntheticFabric` and `SyntheticRunner`) actually correct?**
  _`StateStore` has 9 INFERRED edges - model-reasoned connections that need verification._

## Extraction Audit
- Corpus: 318 files; 2 sensitive files skipped. All 263 documents processed in 12 chunks.
- Static analysis only; no live Fabric, Telegram, model-quality, or sandbox acceptance was tested.
- Bundled Fabric references describe upstream capabilities; their inclusion does not establish that Ray implements them.
- The bundled Synapse capacity-sizing and Fabric cost-estimation references disagree on Spark vCores per CU. This is retained as an AMBIGUOUS relationship, not resolved as a current product fact.
- No AST entities were extracted from these metadata JSON files: `docs\verification.json`, `evaluations\scenarios.json`, `src\ray_de\upstream\source.json`.
- The graph is undirected, with original direction in `_src`/`_tgt`. External or unresolved endpoints are omitted; parallel relationships may collapse.
- Token usage and monetary cost are unavailable from the host tools. See `audit.json` for coverage and limitations.
- HTML includes the integrity-verified vis-network 9.1.6 library for offline opening; inline JavaScript passes `node --check`. Browser rendering could not be checked because no browser was available.
