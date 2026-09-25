//! Persisted UI configuration. Saving never starts capture or writes business data.
use crate::{
    Store,
    auth::admin,
    catalog::{CatalogTable, EndpointRole},
    error::{Error, Result},
    registry::{ConnectorDescriptor, SinkRegistry, SourceRegistry, catalog_fingerprint},
    secrets,
    store::now,
};
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::collections::{BTreeMap, BTreeSet};

pub(crate) const TASK_PLAN_VERSION: &str = "cdc.column-conversion-plan.v1";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct TableMapping {
    pub source_schema: String,
    pub source_table: String,
    pub sink_schema: String,
    pub sink_table: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<String>,
    /// Explicit per-source-column conversion parameters.  Keeping these in
    /// the mapping JSON makes the user-selected parameters part of the task
    /// configuration and lets requalification rebuild the same plan.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub conversion_options: BTreeMap<String, BTreeMap<String, String>>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TaskInput {
    #[serde(default)]
    pub draft_id: Option<String>,
    pub name: String,
    pub source_id: String,
    pub sink_id: String,
    #[serde(default)]
    pub source_database: String,
    #[serde(default)]
    pub sink_database: String,
    pub source_revision: i64,
    pub sink_revision: i64,
    pub start_mode: String,
    pub mappings: Vec<TableMapping>,
    #[serde(default)]
    pub confirmations: Vec<change_event::RiskConfirmation>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FieldPreviewInput {
    pub draft_id: String,
    pub source_id: String,
    pub sink_id: String,
    #[serde(default)]
    pub source_database: String,
    #[serde(default)]
    pub sink_database: String,
    pub source_revision: i64,
    pub sink_revision: i64,
    pub schema: String,
    pub table: String,
    pub column: String,
    #[serde(default)]
    pub parameters: BTreeMap<String, String>,
    #[serde(default)]
    pub confirmations: Vec<change_event::RiskConfirmation>,
}

#[derive(Debug, Serialize)]
pub(crate) struct CompatibilityRuleOptions {
    pub rule: change_event::RuleReference,
    pub options: Vec<change_event::OptionSpec>,
}

#[derive(Serialize)]
pub(crate) struct FieldCompatibilityPreview {
    pub source: crate::catalog::CatalogColumn,
    pub target: crate::catalog::CatalogColumn,
    pub result: Option<change_event::CompatibilityResult>,
    pub error: Option<FieldCompatibilityPreviewError>,
    pub candidates: Vec<CompatibilityRuleOptions>,
}

#[derive(Debug, Serialize)]
pub(crate) struct FieldCompatibilityPreviewError {
    pub class: change_event::FailureClass,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct TaskPlanSnapshot {
    pub plan_version: String,
    pub configuration_revision: i64,
    pub source_metadata_fingerprint: String,
    pub sink_metadata_fingerprint: String,
    pub connector_summary_json: String,
    pub capability_summary_json: String,
    pub capability_manifest_digest: String,
    pub rule_summary_digest: String,
    pub plan_set_digest: String,
    pub plans: Vec<change_event::ColumnConversionPlan>,
    pub confirmations: Vec<change_event::RiskConfirmation>,
}
#[derive(Clone, Serialize)]
pub(crate) struct ReplicationTask {
    pub id: String,
    pub name: String,
    pub source_id: String,
    pub sink_id: String,
    pub source_database: String,
    pub sink_database: String,
    pub source_name: String,
    pub sink_name: String,
    pub source_revision: i64,
    pub sink_revision: i64,
    pub start_mode: String,
    pub mappings: Vec<TableMapping>,
    pub status: String,
    pub created_at: i64,
    pub configuration_changed: bool,
    pub auto_start: bool,
    pub plan_version: Option<String>,
    pub plan_status: String,
    pub plan_invalid_reason: Option<String>,
    pub configuration_revision: i64,
    pub desired_configuration_revision: i64,
    pub effective_configuration_revision: Option<i64>,
    pub source_metadata_fingerprint: Option<String>,
    pub sink_metadata_fingerprint: Option<String>,
    pub connector_summary_json: String,
    pub capability_summary_json: String,
    pub capability_manifest_digest: Option<String>,
    pub rule_summary_digest: Option<String>,
    pub plan_set_digest: Option<String>,
    pub plans: Vec<change_event::ColumnConversionPlan>,
    pub risk_confirmations: Vec<change_event::RiskConfirmation>,
    pub plan_needs_requalification: bool,
    pub runtime: crate::runtime_store::RuntimeInfo,
}
const FIELDS: &str = "t.id,t.name,t.source_id,t.sink_id,t.source_database,t.sink_database,s.name,d.name,t.source_revision,t.sink_revision,t.start_mode,t.mappings_json,t.status,t.created_at,(t.source_revision<>s.revision OR t.sink_revision<>d.revision),t.auto_start,t.plan_version,t.plan_status,t.plan_invalid_reason,t.configuration_revision,t.desired_configuration_revision,t.effective_configuration_revision,t.source_metadata_fingerprint,t.sink_metadata_fingerprint,t.connector_summary_json,t.capability_summary_json,t.capability_manifest_digest,t.rule_summary_digest,t.plan_set_digest,t.plans_json,t.risk_confirmations_json";
const JOINS: &str =
    "replication_tasks t JOIN instances s ON s.id=t.source_id JOIN instances d ON d.id=t.sink_id";
fn task_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<ReplicationTask> {
    let json: String = r.get(11)?;
    let mappings = serde_json::from_str(&json).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(11, rusqlite::types::Type::Text, Box::new(e))
    })?;
    let plans_json: String = r.get(29)?;
    let plans = serde_json::from_str(&plans_json).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(29, rusqlite::types::Type::Text, Box::new(e))
    })?;
    let confirmations_json: String = r.get(30)?;
    let risk_confirmations = serde_json::from_str(&confirmations_json).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(30, rusqlite::types::Type::Text, Box::new(e))
    })?;
    let configuration_changed: bool = r.get(14)?;
    let stored_plan_status: String = r.get(17)?;
    let plan_status = if configuration_changed && stored_plan_status == "valid" {
        "stale".to_owned()
    } else {
        stored_plan_status
    };
    let stored_plan_invalid_reason: Option<String> = r.get(18)?;
    let plan_invalid_reason = if plan_status == "stale" && stored_plan_invalid_reason.is_none() {
        Some("关联实例配置已变化，请重新预检".to_owned())
    } else {
        stored_plan_invalid_reason
    };
    Ok(ReplicationTask {
        id: r.get(0)?,
        name: r.get(1)?,
        source_id: r.get(2)?,
        sink_id: r.get(3)?,
        source_database: r.get(4)?,
        sink_database: r.get(5)?,
        source_name: r.get(6)?,
        sink_name: r.get(7)?,
        source_revision: r.get(8)?,
        sink_revision: r.get(9)?,
        start_mode: r.get(10)?,
        mappings,
        status: r.get(12)?,
        created_at: r.get(13)?,
        configuration_changed,
        auto_start: r.get(15)?,
        plan_version: r.get(16)?,
        plan_status: plan_status.clone(),
        plan_invalid_reason,
        configuration_revision: r.get(19)?,
        desired_configuration_revision: r.get(20)?,
        effective_configuration_revision: r.get(21)?,
        source_metadata_fingerprint: r.get(22)?,
        sink_metadata_fingerprint: r.get(23)?,
        connector_summary_json: r.get(24)?,
        capability_summary_json: r.get(25)?,
        capability_manifest_digest: r.get(26)?,
        rule_summary_digest: r.get(27)?,
        plan_set_digest: r.get(28)?,
        plans,
        risk_confirmations,
        plan_needs_requalification: plan_status != "valid",
        runtime: crate::runtime_store::RuntimeInfo::default(),
    })
}

fn optional_database(value: &str) -> Option<&str> {
    (!value.is_empty()).then_some(value)
}

fn server_build_identity(
    connector: &ConnectorDescriptor,
    metadata: &crate::model::Metadata,
) -> change_event::ServerBuildIdentity {
    if let crate::model::Metadata::Postgresql(metadata) = metadata
        && let Some(server_build) = &metadata.server_build
    {
        return server_build.clone();
    }
    let server_version = match metadata {
        crate::model::Metadata::Mysql { server_version, .. } => server_version,
        crate::model::Metadata::Postgresql(metadata) => &metadata.server_version,
    };
    change_event::ServerBuildIdentity::new(
        connector.identity.kind,
        connector.identity.kind,
        connector.identity.version,
        server_version.clone(),
    )
}

fn source_environment_fingerprint(metadata: &crate::model::Metadata) -> Option<&str> {
    match metadata {
        crate::model::Metadata::Postgresql(metadata) => metadata.environment_fingerprint.as_deref(),
        crate::model::Metadata::Mysql { .. } => None,
    }
}

fn validate_database_selection(connector: &ConnectorDescriptor, database: &str) -> Result<()> {
    if connector.identity.kind == "postgresql" {
        if database.is_empty() {
            return Err(Error::Invalid("PostgreSQL 任务必须选择连接数据库"));
        }
    } else if !database.is_empty() {
        return Err(Error::Invalid("MySQL 任务不需要选择连接数据库"));
    }
    Ok(())
}
pub(crate) fn validate_input(input: &TaskInput) -> Result<()> {
    if let Some(draft_id) = &input.draft_id
        && (draft_id.is_empty()
            || draft_id.len() > 128
            || !draft_id.chars().all(|character| {
                character.is_ascii_alphanumeric() || matches!(character, '-' | '_')
            }))
    {
        return Err(Error::Invalid("任务草稿标识无效"));
    }
    if input.name.trim().is_empty()
        || input.name.len() > 128
        || input.name.chars().any(char::is_control)
    {
        return Err(Error::Invalid(
            "任务名称需要 1–128 字节，且不能包含控制字符",
        ));
    }
    if input.source_id == input.sink_id {
        return Err(Error::Invalid("源端与目的端不能是同一实例"));
    }
    if !["auto", "gtid", "binlog"].contains(&input.start_mode.as_str()) {
        return Err(Error::Invalid("起点模式无效"));
    }
    if input.mappings.is_empty() || input.mappings.len() > 100 {
        return Err(Error::Invalid("请选择 1–100 张表"));
    }
    let mut sources = BTreeSet::new();
    let mut sinks = BTreeSet::new();
    for m in &input.mappings {
        for name in [
            &m.source_schema,
            &m.source_table,
            &m.sink_schema,
            &m.sink_table,
        ] {
            if name.is_empty() || name.chars().count() > 64 || name.chars().any(char::is_control) {
                return Err(Error::Invalid("库表名称无效"));
            }
        }
        let mut columns = BTreeSet::new();
        if m.columns.len() > 4096 {
            return Err(Error::Invalid("单表选择字段过多"));
        }
        for column in &m.columns {
            if column.is_empty()
                || column.chars().count() > 64
                || column.chars().any(char::is_control)
                || !columns.insert(column)
            {
                return Err(Error::Invalid("字段名称无效或重复"));
            }
        }
        if [
            "mysql",
            "sys",
            "information_schema",
            "performance_schema",
            "cdc",
        ]
        .contains(&m.source_schema.to_ascii_lowercase().as_str())
        {
            return Err(Error::Invalid("不能同步系统库"));
        }
        if m.source_schema != m.sink_schema || m.source_table != m.sink_table {
            return Err(Error::Invalid("当前写入模块仅支持同库同名表映射"));
        }
        if !sources.insert((&m.source_schema, &m.source_table))
            || !sinks.insert((&m.sink_schema, &m.sink_table))
        {
            return Err(Error::Invalid("库表映射重复"));
        }
    }
    Ok(())
}
#[cfg(test)]
fn default_source_connector(source: &CatalogTable) -> &'static ConnectorDescriptor {
    let identity = if source.engine.eq_ignore_ascii_case("PostgreSQL") {
        ("postgresql", "15")
    } else {
        ("mysql", "5.7")
    };
    SourceRegistry
        .find(identity.0, identity.1)
        .expect("default Web source connector is registered")
}

#[cfg(test)]
fn default_sink_connector(sink: &CatalogTable) -> &'static ConnectorDescriptor {
    let identity = if sink.engine.eq_ignore_ascii_case("PostgreSQL") {
        ("postgresql", "15")
    } else {
        ("mysql", "5.7")
    };
    SinkRegistry
        .find(identity.0, identity.1)
        .expect("default Web sink connector is registered")
}

fn target_probe_key(schema: &str, table: &str, column: &str) -> String {
    format!("{schema}.{table}.{column}")
}

#[allow(clippy::too_many_arguments)]
fn plan_field_for_sink(
    source_connector: &ConnectorDescriptor,
    sink_connector: &ConnectorDescriptor,
    source: &CatalogTable,
    sink: &CatalogTable,
    source_column: &crate::catalog::CatalogColumn,
    sink_column: &crate::catalog::CatalogColumn,
    route_id: &str,
    configuration_revision: &str,
    parameters: &BTreeMap<String, String>,
    confirmations: &[change_event::RiskConfirmation],
    source_build: Option<change_event::ServerBuildIdentity>,
    target_build: Option<change_event::ServerBuildIdentity>,
    source_type_catalog: Option<&postgresql_15::SourceTypeCatalog>,
    source_environment_fingerprint: Option<&str>,
    target_probe: Option<&change_event::TargetCapabilityProbe>,
    allow_unconfirmed: bool,
) -> Result<change_event::ColumnConversionPlan> {
    let fail =
        |reason: &str| Error::Validation(format!("{}.{}：{reason}", source.schema, source.name));
    let compatibility = crate::registry::field_compatibility_with_source_evidence_and_target_probe(
        source_connector,
        sink_connector,
        source,
        sink,
        source_column,
        sink_column,
        route_id,
        configuration_revision,
        source_build,
        target_build,
        source_type_catalog,
        source_environment_fingerprint,
        parameters,
        confirmations,
        target_probe,
    )
    .map_err(|error| fail(&format!("字段 {} 无法规划：{error}", source_column.name)))?;
    if !matches!(
        compatibility.status,
        change_event::CompatibilityStatus::Compatible
    ) && !(allow_unconfirmed
        && matches!(
            compatibility.status,
            change_event::CompatibilityStatus::NeedsConfirmation
        ))
    {
        return Err(fail(&format!(
            "字段 {} 无法规划：{}",
            source_column.name, compatibility.explanation
        )));
    }
    compatibility.plan.ok_or_else(|| {
        fail(&format!(
            "字段 {} 缺少 ColumnConversionPlan",
            source_column.name
        ))
    })
}

#[allow(clippy::too_many_arguments)]
fn plan_pair_for_sink(
    source_connector: &ConnectorDescriptor,
    sink_connector: &ConnectorDescriptor,
    source: &CatalogTable,
    sink: &CatalogTable,
    route_id: &str,
    configuration_revision: &str,
    conversion_options: &BTreeMap<String, BTreeMap<String, String>>,
    confirmations: &[change_event::RiskConfirmation],
    source_build: Option<change_event::ServerBuildIdentity>,
    target_build: Option<change_event::ServerBuildIdentity>,
    source_type_catalog: Option<&postgresql_15::SourceTypeCatalog>,
    source_environment_fingerprint: Option<&str>,
    target_probes: &BTreeMap<String, change_event::TargetCapabilityProbe>,
    allow_unconfirmed: bool,
) -> Result<Vec<change_event::ColumnConversionPlan>> {
    let fail =
        |reason: &str| Error::Validation(format!("{}.{}：{reason}", source.schema, source.name));
    if let Some(reason) = source.unavailable_reason() {
        return Err(fail(reason));
    }
    if let Some(reason) = sink.unavailable_reason() {
        return Err(fail(&format!("目的表{reason}")));
    }
    if source.primary_key != sink.primary_key {
        return Err(fail("源表与目的表主键不一致"));
    }
    if source.columns.len() != sink.columns.len() {
        return Err(fail("源表与目的表列数不一致"));
    }
    let mut plans = Vec::with_capacity(source.columns.len());
    for (a, b) in source.columns.iter().zip(&sink.columns) {
        if a.name != b.name {
            return Err(fail(&format!(
                "列 {} 的名称、类型、空值、排序规则或生成属性不一致",
                a.name
            )));
        }
        plans.push(plan_field_for_sink(
            source_connector,
            sink_connector,
            source,
            sink,
            a,
            b,
            route_id,
            configuration_revision,
            conversion_options.get(&a.name).unwrap_or(&BTreeMap::new()),
            confirmations,
            source_build.clone(),
            target_build.clone(),
            source_type_catalog,
            source_environment_fingerprint,
            target_probes.get(&target_probe_key(&sink.schema, &sink.name, &b.name)),
            allow_unconfirmed,
        )?);
    }
    Ok(plans)
}

#[cfg_attr(not(test), allow(dead_code))]
fn validate_pair_for_sink(
    source_connector: &ConnectorDescriptor,
    sink_connector: &ConnectorDescriptor,
    source: &CatalogTable,
    sink: &CatalogTable,
) -> Result<()> {
    plan_pair_for_sink(
        source_connector,
        sink_connector,
        source,
        sink,
        "web-preflight",
        "catalog",
        &BTreeMap::new(),
        &[],
        None,
        None,
        None,
        None,
        &BTreeMap::new(),
        false,
    )
    .map(|_| ())
}

#[cfg(test)]
pub(crate) fn validate_pair(source: &CatalogTable, sink: &CatalogTable) -> Result<()> {
    validate_pair_for_sink(
        default_source_connector(source),
        default_sink_connector(sink),
        source,
        sink,
    )
}

#[allow(clippy::too_many_arguments)]
fn plan_selected_pair_for_sink(
    source_connector: &ConnectorDescriptor,
    sink_connector: &ConnectorDescriptor,
    source: &CatalogTable,
    sink: &CatalogTable,
    columns: &[String],
    route_id: &str,
    configuration_revision: &str,
    conversion_options: &BTreeMap<String, BTreeMap<String, String>>,
    confirmations: &[change_event::RiskConfirmation],
    source_build: Option<change_event::ServerBuildIdentity>,
    target_build: Option<change_event::ServerBuildIdentity>,
    source_type_catalog: Option<&postgresql_15::SourceTypeCatalog>,
    source_environment_fingerprint: Option<&str>,
    target_probes: &BTreeMap<String, change_event::TargetCapabilityProbe>,
    allow_unconfirmed: bool,
) -> Result<Vec<change_event::ColumnConversionPlan>> {
    if columns.is_empty() {
        return plan_pair_for_sink(
            source_connector,
            sink_connector,
            source,
            sink,
            route_id,
            configuration_revision,
            conversion_options,
            confirmations,
            source_build,
            target_build,
            source_type_catalog,
            source_environment_fingerprint,
            target_probes,
            allow_unconfirmed,
        );
    }
    let fail =
        |reason: &str| Error::Validation(format!("{}.{}：{reason}", source.schema, source.name));
    if let Some(reason) = source.unavailable_reason() {
        return Err(fail(reason));
    }
    if let Some(reason) = sink.unavailable_reason() {
        return Err(fail(&format!("目的表{reason}")));
    }
    if source.primary_key != sink.primary_key {
        return Err(fail("源表与目的表主键不一致"));
    }

    let selected = columns.iter().map(String::as_str).collect::<BTreeSet<_>>();
    for key in &source.primary_key {
        if !selected.contains(key.as_str()) {
            return Err(fail(&format!("字段选择必须包含主键 {key}")));
        }
    }
    let mut plans = Vec::with_capacity(selected.len());
    for name in &selected {
        let a = source
            .columns
            .iter()
            .find(|column| column.name == *name)
            .ok_or_else(|| fail(&format!("源字段 {name} 不存在")))?;
        let b = sink
            .columns
            .iter()
            .find(|column| column.name == *name)
            .ok_or_else(|| fail(&format!("目的字段 {name} 不存在")))?;
        plans.push(plan_field_for_sink(
            source_connector,
            sink_connector,
            source,
            sink,
            a,
            b,
            route_id,
            configuration_revision,
            conversion_options.get(*name).unwrap_or(&BTreeMap::new()),
            confirmations,
            source_build.clone(),
            target_build.clone(),
            source_type_catalog,
            source_environment_fingerprint,
            target_probes.get(&target_probe_key(&sink.schema, &sink.name, &b.name)),
            allow_unconfirmed,
        )?);
    }
    for column in &sink.columns {
        let generated = column.extra.to_ascii_lowercase();
        if !selected.contains(column.name.as_str())
            && !column.nullable
            && column.default_value.is_none()
            && !generated.contains("auto_increment")
            && !generated.contains("generated")
        {
            return Err(fail(&format!(
                "目的字段 {} 未选择且没有默认值，INSERT 将无法执行",
                column.name
            )));
        }
    }
    Ok(plans)
}

#[cfg_attr(not(test), allow(dead_code))]
fn validate_selected_pair_for_sink(
    source_connector: &ConnectorDescriptor,
    sink_connector: &ConnectorDescriptor,
    source: &CatalogTable,
    sink: &CatalogTable,
    columns: &[String],
) -> Result<()> {
    plan_selected_pair_for_sink(
        source_connector,
        sink_connector,
        source,
        sink,
        columns,
        "web-preflight",
        "catalog",
        &BTreeMap::new(),
        &[],
        None,
        None,
        None,
        None,
        &BTreeMap::new(),
        false,
    )
    .map(|_| ())
}

#[cfg(test)]
pub(crate) fn validate_selected_pair(
    source: &CatalogTable,
    sink: &CatalogTable,
    columns: &[String],
) -> Result<()> {
    validate_selected_pair_for_sink(
        default_source_connector(source),
        default_sink_connector(sink),
        source,
        sink,
        columns,
    )
}

fn digest_serialized<T: Serialize>(value: &T) -> String {
    let bytes = serde_json::to_vec(value).expect("task plan inputs are serializable");
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Recompute the digest that binds the runtime to one immutable plan set.
/// Keeping this calculation next to plan persistence prevents the worker from
/// accepting a collection whose individual plan digests are valid but whose
/// route-level summary was swapped or reordered.
pub(crate) fn computed_plan_set_digest(task: &ReplicationTask) -> String {
    let source_metadata_fingerprint = task.source_metadata_fingerprint.clone().unwrap_or_default();
    let sink_metadata_fingerprint = task.sink_metadata_fingerprint.clone().unwrap_or_default();
    let plan_digests = task
        .plans
        .iter()
        .map(|plan| plan.plan_digest.clone())
        .collect::<Vec<_>>();
    let rule_summary = task
        .plans
        .iter()
        .map(|plan| {
            (
                plan.capability_code.clone(),
                plan.rule.clone(),
                plan.parameters.clone(),
                plan.failure_policy,
            )
        })
        .collect::<Vec<_>>();
    let rule_summary_digest = digest_serialized(&rule_summary);
    digest_serialized(&(
        TASK_PLAN_VERSION,
        &task.id,
        task.configuration_revision,
        &source_metadata_fingerprint,
        &sink_metadata_fingerprint,
        &task.connector_summary_json,
        &task.capability_summary_json,
        &rule_summary_digest,
        &plan_digests,
        &task.risk_confirmations,
    ))
}

#[allow(clippy::too_many_arguments)]
fn plan_snapshot(
    route_id: &str,
    configuration_revision: i64,
    source_connector: &ConnectorDescriptor,
    sink_connector: &ConnectorDescriptor,
    mappings: &[TableMapping],
    source_tables: &BTreeMap<(String, String), CatalogTable>,
    sink_tables: &BTreeMap<(String, String), CatalogTable>,
    source_metadata: &crate::model::Metadata,
    sink_metadata: &crate::model::Metadata,
    source_type_catalog_digest: Option<&str>,
    plans: Vec<change_event::ColumnConversionPlan>,
    confirmations: &[change_event::RiskConfirmation],
    source_build: change_event::ServerBuildIdentity,
    target_build: change_event::ServerBuildIdentity,
) -> Result<TaskPlanSnapshot> {
    let source_catalog = mappings
        .iter()
        .map(|mapping| {
            let table = source_tables
                .get(&(mapping.source_schema.clone(), mapping.source_table.clone()))
                .expect("source table was checked during preflight");
            (
                format!("{}.{}", mapping.source_schema, mapping.source_table),
                catalog_fingerprint(table),
            )
        })
        .collect::<Vec<_>>();
    let sink_catalog = mappings
        .iter()
        .map(|mapping| {
            let table = sink_tables
                .get(&(mapping.sink_schema.clone(), mapping.sink_table.clone()))
                .expect("sink table was checked during preflight");
            (
                format!("{}.{}", mapping.sink_schema, mapping.sink_table),
                catalog_fingerprint(table),
            )
        })
        .collect::<Vec<_>>();
    // Catalog tables alone do not cover the evidence used by the source type
    // mapper or the target session. Keep the complete probe metadata in the
    // snapshot so a requalification observes extension, type-directory,
    // server/environment, and target-session changes as stale inputs.
    let source_metadata_fingerprint =
        digest_serialized(&(source_metadata, source_type_catalog_digest, source_catalog));
    let target_probe_digests = plans
        .iter()
        .map(|plan| {
            (
                plan.target_field.lineage_id.clone(),
                plan.target_probe_digest.clone(),
            )
        })
        .collect::<Vec<_>>();
    let sink_metadata_fingerprint =
        digest_serialized(&(sink_metadata, sink_catalog, target_probe_digests));
    let manifest_build = plans
        .iter()
        .find_map(|plan| plan.target_build.clone())
        .unwrap_or(target_build);
    let manifest = sink_connector.structured_manifest(manifest_build.clone());
    let connector_summary_json = serde_json::to_string(&serde_json::json!({
        "source": source_connector.identity,
        "sink": sink_connector.identity,
        "source_build": source_build,
        "target_build": manifest_build,
    }))
    .map_err(|_| Error::Internal)?;
    let capability_summary_json = serde_json::to_string(&serde_json::json!({
        "connector": manifest.connector,
        "target_build": manifest.target_build,
        "contract": manifest.contract,
        "capability_codes": manifest.capabilities.iter().map(|entry| entry.code.clone()).collect::<Vec<_>>(),
    }))
    .map_err(|_| Error::Internal)?;
    let rule_summary = plans
        .iter()
        .map(|plan| {
            (
                plan.capability_code.clone(),
                plan.rule.clone(),
                plan.parameters.clone(),
                plan.failure_policy,
            )
        })
        .collect::<Vec<_>>();
    let rule_summary_digest = digest_serialized(&rule_summary);
    let plan_digests = plans
        .iter()
        .map(|plan| plan.plan_digest.clone())
        .collect::<Vec<_>>();
    let plan_set_digest = digest_serialized(&(
        TASK_PLAN_VERSION,
        route_id,
        configuration_revision,
        &source_metadata_fingerprint,
        &sink_metadata_fingerprint,
        &connector_summary_json,
        &capability_summary_json,
        &rule_summary_digest,
        &plan_digests,
        confirmations,
    ));
    Ok(TaskPlanSnapshot {
        plan_version: TASK_PLAN_VERSION.into(),
        configuration_revision,
        source_metadata_fingerprint,
        sink_metadata_fingerprint,
        connector_summary_json,
        capability_summary_json,
        capability_manifest_digest: manifest.digest,
        rule_summary_digest,
        plan_set_digest,
        plans,
        confirmations: confirmations.to_vec(),
    })
}

fn persist_plan_rows(
    tx: &rusqlite::Transaction<'_>,
    task_id: &str,
    snapshot: &TaskPlanSnapshot,
) -> Result<()> {
    tx.execute(
        "DELETE FROM task_conversion_plans WHERE task_id=?1",
        [task_id],
    )?;
    for (ordinal, plan) in snapshot.plans.iter().enumerate() {
        tx.execute(
            "INSERT INTO task_conversion_plans(task_id,ordinal,source_field_lineage,target_field_lineage,plan_digest,plan_json) VALUES(?1,?2,?3,?4,?5,?6)",
            params![
                task_id,
                i64::try_from(ordinal).map_err(|_| Error::Internal)?,
                plan.source_field.lineage_id,
                plan.target_field.lineage_id,
                plan.plan_digest,
                serde_json::to_string(plan).map_err(|_| Error::Internal)?,
            ],
        )?;
    }
    Ok(())
}

fn persist_plan_snapshot(
    tx: &rusqlite::Transaction<'_>,
    task_id: &str,
    snapshot: &TaskPlanSnapshot,
) -> Result<()> {
    let plans_json = serde_json::to_string(&snapshot.plans).map_err(|_| Error::Internal)?;
    let confirmations_json =
        serde_json::to_string(&snapshot.confirmations).map_err(|_| Error::Internal)?;
    tx.execute(
        "UPDATE replication_tasks SET plan_version=?2,plan_status='valid',plan_invalid_reason=NULL,configuration_revision=?3,desired_configuration_revision=?3,source_metadata_fingerprint=?4,sink_metadata_fingerprint=?5,connector_summary_json=?6,capability_summary_json=?7,capability_manifest_digest=?8,rule_summary_digest=?9,plan_set_digest=?10,plans_json=?11,risk_confirmations_json=?12 WHERE id=?1",
        params![
            task_id,
            snapshot.plan_version,
            snapshot.configuration_revision,
            snapshot.source_metadata_fingerprint,
            snapshot.sink_metadata_fingerprint,
            snapshot.connector_summary_json,
            snapshot.capability_summary_json,
            snapshot.capability_manifest_digest,
            snapshot.rule_summary_digest,
            snapshot.plan_set_digest,
            plans_json,
            confirmations_json,
        ],
    )?;
    persist_plan_rows(tx, task_id, snapshot)
}

fn persist_configuration_revision(
    tx: &rusqlite::Transaction<'_>,
    task_id: &str,
    actor: i64,
    input: &TaskInput,
    snapshot: Option<&TaskPlanSnapshot>,
) -> Result<()> {
    let revision = snapshot.map_or(1, |snapshot| snapshot.configuration_revision);
    let mappings_json = serde_json::to_string(&input.mappings).map_err(|_| Error::Internal)?;
    let (
        plan_version,
        plan_status,
        plan_invalid_reason,
        source_metadata_fingerprint,
        sink_metadata_fingerprint,
        connector_summary_json,
        capability_summary_json,
        capability_manifest_digest,
        rule_summary_digest,
        plan_set_digest,
        plans_json,
        risk_confirmations_json,
    ) = if let Some(snapshot) = snapshot {
        (
            Some(snapshot.plan_version.clone()),
            "valid".to_owned(),
            None::<String>,
            Some(snapshot.source_metadata_fingerprint.clone()),
            Some(snapshot.sink_metadata_fingerprint.clone()),
            snapshot.connector_summary_json.clone(),
            snapshot.capability_summary_json.clone(),
            Some(snapshot.capability_manifest_digest.clone()),
            Some(snapshot.rule_summary_digest.clone()),
            Some(snapshot.plan_set_digest.clone()),
            serde_json::to_string(&snapshot.plans).map_err(|_| Error::Internal)?,
            serde_json::to_string(&snapshot.confirmations).map_err(|_| Error::Internal)?,
        )
    } else {
        (
            None,
            "legacy".to_owned(),
            None,
            None,
            None,
            "{}".to_owned(),
            "{}".to_owned(),
            None,
            None,
            None,
            "[]".to_owned(),
            serde_json::to_string(&input.confirmations).map_err(|_| Error::Internal)?,
        )
    };
    tx.execute(
        "INSERT INTO task_configuration_revisions(
             task_id,revision,name,source_id,sink_id,source_database,sink_database,
             source_revision,sink_revision,start_mode,mappings_json,plan_version,plan_status,
             plan_invalid_reason,source_metadata_fingerprint,sink_metadata_fingerprint,
             connector_summary_json,capability_summary_json,capability_manifest_digest,
             rule_summary_digest,plan_set_digest,plans_json,risk_confirmations_json,
             created_at,created_by
         ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24,?25)",
        params![
            task_id,
            revision,
            input.name.trim(),
            input.source_id,
            input.sink_id,
            input.source_database,
            input.sink_database,
            input.source_revision,
            input.sink_revision,
            input.start_mode,
            mappings_json,
            plan_version,
            plan_status,
            plan_invalid_reason,
            source_metadata_fingerprint,
            sink_metadata_fingerprint,
            connector_summary_json,
            capability_summary_json,
            capability_manifest_digest,
            rule_summary_digest,
            plan_set_digest,
            plans_json,
            risk_confirmations_json,
            now(),
            actor,
        ],
    )?;
    Ok(())
}

fn input_from_task(task: &ReplicationTask) -> TaskInput {
    TaskInput {
        draft_id: Some(task.id.clone()),
        name: task.name.clone(),
        source_id: task.source_id.clone(),
        sink_id: task.sink_id.clone(),
        source_database: task.source_database.clone(),
        sink_database: task.sink_database.clone(),
        source_revision: task.source_revision,
        sink_revision: task.sink_revision,
        start_mode: task.start_mode.clone(),
        mappings: task.mappings.clone(),
        confirmations: task.risk_confirmations.clone(),
    }
}

impl Store {
    pub(crate) fn authoritative_confirmations(
        &self,
        actor: i64,
        confirmations: &[change_event::RiskConfirmation],
    ) -> Result<Vec<change_event::RiskConfirmation>> {
        let conn = self.db()?;
        admin(&conn, actor)?;
        let username: String = conn
            .query_row("SELECT username FROM users WHERE id=?1", [actor], |row| {
                row.get(0)
            })
            .optional()?
            .ok_or(Error::Forbidden)?;
        let confirmed_at = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        Ok(confirmations
            .iter()
            .cloned()
            .map(|mut confirmation| {
                confirmation.actor.clone_from(&username);
                confirmation.confirmed_at.clone_from(&confirmed_at);
                confirmation
            })
            .collect())
    }

    pub(crate) fn tasks(&self) -> Result<Vec<ReplicationTask>> {
        let conn = self.db()?;
        let mut tasks: Vec<ReplicationTask> = conn
            .prepare(&format!(
                "SELECT {FIELDS} FROM {JOINS} ORDER BY t.created_at DESC,t.id"
            ))?
            .query_map([], task_row)?
            .collect::<rusqlite::Result<_>>()?;
        drop(conn);
        for task in &mut tasks {
            self.load_runtime(task)?;
        }
        Ok(tasks)
    }
    pub(crate) fn task(&self, id: &str) -> Result<ReplicationTask> {
        let mut task = self
            .db()?
            .query_row(
                &format!("SELECT {FIELDS} FROM {JOINS} WHERE t.id=?1"),
                [id],
                task_row,
            )
            .optional()?
            .ok_or(Error::NotFound)?;
        self.load_runtime(&mut task)?;
        Ok(task)
    }

    pub(crate) fn preview_field(
        &self,
        actor: i64,
        input: FieldPreviewInput,
    ) -> Result<FieldCompatibilityPreview> {
        if input.draft_id.is_empty()
            || input.draft_id.len() > 128
            || !input.draft_id.chars().all(|character| {
                character.is_ascii_alphanumeric() || matches!(character, '-' | '_')
            })
        {
            return Err(Error::Invalid("任务草稿标识无效"));
        }
        if input.schema.is_empty()
            || input.table.is_empty()
            || input.column.is_empty()
            || [
                input.schema.as_str(),
                input.table.as_str(),
                input.column.as_str(),
            ]
            .iter()
            .any(|value| value.chars().count() > 64 || value.chars().any(char::is_control))
        {
            return Err(Error::Invalid("库表字段名称无效"));
        }
        {
            let conn = self.db()?;
            admin(&conn, actor)?;
        }
        let source_connector = self.connector(&input.source_id, EndpointRole::Source)?;
        let sink_connector = self.connector(&input.sink_id, EndpointRole::Sink)?;
        validate_database_selection(source_connector, &input.source_database)?;
        validate_database_selection(sink_connector, &input.sink_database)?;
        let mut source = self.catalog_connection_for_database(
            actor,
            &input.source_id,
            EndpointRole::Source,
            optional_database(&input.source_database),
        )?;
        let mut sink = self.catalog_connection_for_database(
            actor,
            &input.sink_id,
            EndpointRole::Sink,
            optional_database(&input.sink_database),
        )?;
        if source.server_uuid == sink.server_uuid {
            return Err(Error::Invalid("源端与目的端不能指向同一个数据库实例"));
        }
        if source.revision != input.source_revision || sink.revision != input.sink_revision {
            return Err(Error::Conflict("实例配置已变化，请重新选择实例并加载库表"));
        }
        let source_table = source
            .tables(&input.schema)?
            .into_iter()
            .find(|table| table.name == input.table)
            .ok_or(Error::Validation("源表不存在或无权访问".into()))?;
        let sink_table = sink
            .tables(&input.schema)?
            .into_iter()
            .find(|table| table.name == input.table)
            .ok_or(Error::Validation("目的表不存在或无权访问".into()))?;
        let source_column = source_table
            .columns
            .iter()
            .find(|column| column.name == input.column)
            .cloned()
            .ok_or(Error::Validation("源字段不存在或无权访问".into()))?;
        let sink_column = sink_table
            .columns
            .iter()
            .find(|column| column.name == input.column)
            .cloned()
            .ok_or(Error::Validation("目的字段不存在或无权访问".into()))?;
        // Field preview must use the same target-side evidence as task
        // preflight.  Without this probe the dialog could show a manifest
        // candidate that the actual target column, extension, or session
        // cannot qualify.
        let target_probe = sink.probe_target(&input.schema, &input.table, &input.column)?;
        let source_build = server_build_identity(source_connector, &source.metadata);
        let target_build = server_build_identity(sink_connector, &sink.metadata);
        let route_id = input.draft_id;
        let configuration_revision = format!("{route_id}:r1");
        let compatibility =
            crate::registry::field_compatibility_with_source_evidence_and_target_probe(
                source_connector,
                sink_connector,
                &source_table,
                &sink_table,
                &source_column,
                &sink_column,
                &route_id,
                &configuration_revision,
                Some(source_build),
                Some(target_build.clone()),
                source.source_type_catalog.as_ref(),
                source_environment_fingerprint(&source.metadata),
                &input.parameters,
                &input.confirmations,
                Some(&target_probe),
            );
        let (result, error) = match compatibility {
            Ok(result) => (Some(result), None),
            Err(error) => (
                None,
                Some(FieldCompatibilityPreviewError {
                    class: error.class(),
                    code: error.code().to_owned(),
                    message: error.to_string(),
                }),
            ),
        };
        let manifest = sink_connector.structured_manifest(target_build);
        let candidates = result
            .as_ref()
            .into_iter()
            .flat_map(|result| result.candidates.iter())
            .filter_map(|candidate| {
                manifest
                    .capabilities
                    .iter()
                    .find(|entry| {
                        entry.rule.id == candidate.rule.id
                            && entry.rule.version == candidate.rule.version
                    })
                    .map(|entry| CompatibilityRuleOptions {
                        rule: candidate.rule.clone(),
                        options: entry.rule.options.clone(),
                    })
            })
            .collect();
        Ok(FieldCompatibilityPreview {
            source: source_column,
            target: sink_column,
            result,
            error,
            candidates,
        })
    }

    #[allow(dead_code)]
    pub(crate) fn preflight_task(&self, actor: i64, input: &TaskInput) -> Result<()> {
        self.preflight_task_with_snapshot(actor, input, "web-preflight", 1)
            .map(|_| ())
    }

    pub(crate) fn preflight_task_with_snapshot(
        &self,
        actor: i64,
        input: &TaskInput,
        route_id: &str,
        configuration_revision: i64,
    ) -> Result<TaskPlanSnapshot> {
        self.preflight_task_with_snapshot_mode(
            actor,
            input,
            route_id,
            configuration_revision,
            false,
        )
    }

    pub(crate) fn preflight_task_preview(
        &self,
        actor: i64,
        input: &TaskInput,
        route_id: &str,
        configuration_revision: i64,
    ) -> Result<TaskPlanSnapshot> {
        self.preflight_task_with_snapshot_mode(actor, input, route_id, configuration_revision, true)
    }

    fn preflight_task_with_snapshot_mode(
        &self,
        actor: i64,
        input: &TaskInput,
        route_id: &str,
        configuration_revision: i64,
        allow_unconfirmed: bool,
    ) -> Result<TaskPlanSnapshot> {
        {
            let conn = self.db()?;
            admin(&conn, actor)?;
        }
        let confirmations = self.authoritative_confirmations(actor, &input.confirmations)?;
        validate_input(input)?;
        let source_connector = self.connector(&input.source_id, EndpointRole::Source)?;
        let sink_connector = self.connector(&input.sink_id, EndpointRole::Sink)?;
        validate_database_selection(source_connector, &input.source_database)?;
        validate_database_selection(sink_connector, &input.sink_database)?;
        let mut source = self.catalog_connection_for_database(
            actor,
            &input.source_id,
            EndpointRole::Source,
            optional_database(&input.source_database),
        )?;
        let mut sink = self.catalog_connection_for_database(
            actor,
            &input.sink_id,
            EndpointRole::Sink,
            optional_database(&input.sink_database),
        )?;
        if source.server_uuid == sink.server_uuid {
            return Err(Error::Invalid("源端与目的端指向同一个数据库实例"));
        }
        if source.revision != input.source_revision || sink.revision != input.sink_revision {
            return Err(Error::Conflict("实例配置已变化，请重新选择实例并加载库表"));
        }
        if input.start_mode == "gtid" && !source_connector.capabilities.supports_gtid {
            return Err(Error::Invalid("所选源连接器不支持 GTID 起点"));
        }
        if input.start_mode == "binlog" && !source_connector.capabilities.supports_file_position {
            return Err(Error::Invalid("所选源连接器不支持文件位点起点"));
        }
        if let crate::model::Metadata::Mysql {
            log_bin,
            binlog_format,
            binlog_row_image,
            gtid_mode,
            ..
        } = &source.metadata
        {
            if !log_bin || binlog_format != "ROW" || binlog_row_image != "FULL" {
                return Err(Error::Invalid("源端需要开启 binlog，且配置为 ROW / FULL"));
            }
            if input.start_mode == "gtid" && gtid_mode != "ON" {
                return Err(Error::Invalid(
                    "源端未启用 GTID，请使用自动模式或文件 + position",
                ));
            }
        }
        if let crate::model::Metadata::Postgresql(metadata) = &source.metadata
            && !metadata.can_replicate
        {
            return Err(Error::Invalid(
                "源端需要 PostgreSQL Logical Replication 权限",
            ));
        }
        let mut source_tables = BTreeMap::new();
        let mut sink_tables = BTreeMap::new();
        let source_build = server_build_identity(source_connector, &source.metadata);
        let target_build = server_build_identity(sink_connector, &sink.metadata);
        let source_type_catalog = source.source_type_catalog.clone();
        let source_type_catalog_digest = source_type_catalog
            .as_ref()
            .map(postgresql_15::SourceTypeCatalog::evidence_digest);
        let source_environment_fingerprint =
            source_environment_fingerprint(&source.metadata).map(str::to_owned);
        let mut target_probes = BTreeMap::new();
        for schema in input
            .mappings
            .iter()
            .map(|m| &m.source_schema)
            .collect::<BTreeSet<_>>()
        {
            for table in source.tables(schema)? {
                source_tables.insert((table.schema.clone(), table.name.clone()), table);
            }
            for table in sink.tables(schema)? {
                sink_tables.insert((table.schema.clone(), table.name.clone()), table);
            }
        }
        for mapping in &input.mappings {
            let sink_table = sink_tables
                .get(&(mapping.sink_schema.clone(), mapping.sink_table.clone()))
                .ok_or_else(|| {
                    Error::Validation(format!(
                        "目的表 {}.{} 不存在或无权访问",
                        mapping.sink_schema, mapping.sink_table
                    ))
                })?;
            let columns = if mapping.columns.is_empty() {
                sink_table
                    .columns
                    .iter()
                    .map(|column| column.name.as_str())
                    .collect::<Vec<_>>()
            } else {
                mapping.columns.iter().map(String::as_str).collect()
            };
            for column in columns {
                let probe = sink.probe_target(&mapping.sink_schema, &mapping.sink_table, column)?;
                target_probes.insert(
                    target_probe_key(&mapping.sink_schema, &mapping.sink_table, column),
                    probe,
                );
            }
        }
        let configuration_revision_label = format!("{route_id}:r{configuration_revision}");
        let mut plans = Vec::new();
        for m in &input.mappings {
            let a = source_tables
                .get(&(m.source_schema.clone(), m.source_table.clone()))
                .ok_or_else(|| {
                    Error::Validation(format!(
                        "源表 {}.{} 不存在或无权访问",
                        m.source_schema, m.source_table
                    ))
                })?;
            let b = sink_tables
                .get(&(m.sink_schema.clone(), m.sink_table.clone()))
                .ok_or_else(|| {
                    Error::Validation(format!(
                        "目的表 {}.{} 不存在或无权访问；请先建表",
                        m.sink_schema, m.sink_table
                    ))
                })?;
            plans.extend(plan_selected_pair_for_sink(
                source_connector,
                sink_connector,
                a,
                b,
                &m.columns,
                route_id,
                &configuration_revision_label,
                &m.conversion_options,
                &confirmations,
                Some(source_build.clone()),
                Some(target_build.clone()),
                source_type_catalog.as_ref(),
                source_environment_fingerprint.as_deref(),
                &target_probes,
                allow_unconfirmed,
            )?);
        }
        let snapshot = plan_snapshot(
            route_id,
            configuration_revision,
            source_connector,
            sink_connector,
            &input.mappings,
            &source_tables,
            &sink_tables,
            &source.metadata,
            &sink.metadata,
            source_type_catalog_digest.as_deref(),
            plans,
            &confirmations,
            source_build,
            target_build,
        )?;
        Ok(snapshot)
    }
    pub(crate) fn create_task(&self, actor: i64, input: TaskInput) -> Result<ReplicationTask> {
        validate_input(&input)?;
        let id = input.draft_id.clone().unwrap_or_else(secrets::random_token);
        let snapshot = self.preflight_task_with_snapshot(actor, &input, &id, 1)?;
        self.insert_task_with_snapshot(actor, input, id, Some(&snapshot))
    }
    // Called only after database preflight. Recheck authority and connection revisions atomically.
    #[allow(dead_code)]
    pub(crate) fn insert_task(&self, actor: i64, input: TaskInput) -> Result<ReplicationTask> {
        let id = secrets::random_token();
        let mut input = input;
        input.confirmations = self.authoritative_confirmations(actor, &input.confirmations)?;
        self.insert_task_with_snapshot(actor, input, id, None)
    }

    fn insert_task_with_snapshot(
        &self,
        actor: i64,
        input: TaskInput,
        id: String,
        snapshot: Option<&TaskPlanSnapshot>,
    ) -> Result<ReplicationTask> {
        validate_input(&input)?;
        let json = serde_json::to_string(&input.mappings).map_err(|_| Error::Internal)?;
        let mut conn = self.db()?;
        let tx = conn.transaction()?;
        admin(&tx, actor)?;
        for (instance_id, expected, role) in [
            (
                &input.source_id,
                input.source_revision,
                EndpointRole::Source,
            ),
            (&input.sink_id, input.sink_revision, EndpointRole::Sink),
        ] {
            self.connector_in_transaction(&tx, instance_id, role)?;
            let actual: i64 = tx
                .query_row(
                    "SELECT revision FROM instances WHERE id=?1",
                    [instance_id],
                    |r| r.get(0),
                )
                .optional()?
                .ok_or(Error::NotFound)?;
            if actual != expected {
                return Err(Error::Conflict("检查期间实例配置已变化，请重新加载库表"));
            }
        }
        tx.execute("INSERT INTO replication_tasks(id,name,source_id,sink_id,source_database,sink_database,source_revision,sink_revision,start_mode,mappings_json,created_at,created_by) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",
            params![id,input.name.trim(),input.source_id,input.sink_id,input.source_database,input.sink_database,input.source_revision,input.sink_revision,input.start_mode,json,now(),actor])?;
        if let Some(snapshot) = snapshot {
            persist_plan_snapshot(&tx, &id, snapshot)?;
        }
        persist_configuration_revision(&tx, &id, actor, &input, snapshot)?;
        tx.commit()?;
        drop(conn);
        self.task(&id)
    }

    pub(crate) fn requalify_task(
        &self,
        actor: i64,
        id: &str,
        confirmations: Vec<change_event::RiskConfirmation>,
    ) -> Result<ReplicationTask> {
        self.require_admin(actor)?;
        let task = self.task(id)?;
        let active: bool = self.db()?.query_row(
            "SELECT EXISTS(SELECT 1 FROM task_runtime WHERE task_id=?1 AND state IN ('starting','running','stopping'))",
            [id],
            |row| row.get(0),
        )?;
        if active {
            return Err(Error::Conflict("任务运行期间不能重新预检"));
        }
        let (source_revision, sink_revision): (i64, i64) = self.db()?.query_row(
            "SELECT s.revision,d.revision FROM replication_tasks t JOIN instances s ON s.id=t.source_id JOIN instances d ON d.id=t.sink_id WHERE t.id=?1",
            [id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        let mut input = input_from_task(&task);
        input.source_revision = source_revision;
        input.sink_revision = sink_revision;
        input.confirmations = confirmations;
        let next_revision = task.configuration_revision.max(1) + 1;
        let snapshot = match self.preflight_task_with_snapshot(actor, &input, id, next_revision) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                self.mark_plan_stale(id, &error.to_string())?;
                return Err(error);
            }
        };
        let mut conn = self.db()?;
        let tx = conn.transaction()?;
        admin(&tx, actor)?;
        for (instance_id, expected_revision) in [
            (&input.source_id, input.source_revision),
            (&input.sink_id, input.sink_revision),
        ] {
            let actual: i64 = tx
                .query_row(
                    "SELECT revision FROM instances WHERE id=?1",
                    [instance_id],
                    |row| row.get(0),
                )
                .optional()?
                .ok_or(Error::NotFound)?;
            if actual != expected_revision {
                return Err(Error::Conflict("重新预检期间实例配置已变化，请重试"));
            }
        }
        let changed = tx.execute(
            "UPDATE replication_tasks SET source_revision=?2,sink_revision=?3,source_database=?4,sink_database=?5,start_mode=?6,mappings_json=?7 WHERE id=?1 AND configuration_revision=?8 AND desired_configuration_revision=?8",
            params![
                id,
                input.source_revision,
                input.sink_revision,
                input.source_database,
                input.sink_database,
                input.start_mode,
                serde_json::to_string(&input.mappings).map_err(|_| Error::Internal)?,
                task.configuration_revision,
            ],
        )?;
        if changed != 1 {
            return Err(Error::Conflict("重新预检期间任务配置已变化，请重试"));
        }
        persist_plan_snapshot(&tx, id, &snapshot)?;
        persist_configuration_revision(&tx, id, actor, &input, Some(&snapshot))?;
        tx.commit()?;
        drop(conn);
        self.task(id)
    }

    pub(crate) fn preview_requalify_task(&self, actor: i64, id: &str) -> Result<TaskPlanSnapshot> {
        self.require_admin(actor)?;
        let task = self.task(id)?;
        let active: bool = self.db()?.query_row(
            "SELECT EXISTS(SELECT 1 FROM task_runtime WHERE task_id=?1 AND state IN ('starting','running','stopping'))",
            [id],
            |row| row.get(0),
        )?;
        if active {
            return Err(Error::Conflict("任务运行期间不能重新预检"));
        }
        let (source_revision, sink_revision): (i64, i64) = self.db()?.query_row(
            "SELECT s.revision,d.revision FROM replication_tasks t JOIN instances s ON s.id=t.source_id JOIN instances d ON d.id=t.sink_id WHERE t.id=?1",
            [id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        let mut input = input_from_task(&task);
        input.source_revision = source_revision;
        input.sink_revision = sink_revision;
        input.confirmations.clear();
        let next_revision = task.configuration_revision.max(1) + 1;
        self.preflight_task_preview(actor, &input, id, next_revision)
    }

    fn mark_plan_stale(&self, id: &str, reason: &str) -> Result<()> {
        self.db()?.execute(
            "UPDATE replication_tasks SET plan_status='stale',plan_invalid_reason=?2 WHERE id=?1 AND plan_status<>'legacy'",
            params![id, reason.chars().take(512).collect::<String>()],
        )?;
        Ok(())
    }

    pub(crate) fn ensure_saved_plan_current(
        &self,
        actor: i64,
        task: &ReplicationTask,
    ) -> Result<()> {
        if task.configuration_changed {
            self.mark_plan_stale(task.id.as_str(), "关联实例配置已变化，请重新预检")?;
            return Err(Error::Conflict("关联实例配置已变化，请重新预检"));
        }
        if task.plan_status != "valid"
            || task.plan_version.as_deref() != Some(TASK_PLAN_VERSION)
            || task.plans.is_empty()
        {
            return Err(Error::Conflict(
                "任务没有有效的 ColumnConversionPlan，请先重新预检",
            ));
        }
        if task.desired_configuration_revision != task.configuration_revision {
            return Err(Error::Conflict(
                "任务 Desired Configuration revision 不一致，请先重新预检",
            ));
        }
        let mappings_json = serde_json::to_string(&task.mappings).map_err(|_| Error::Internal)?;
        let plans_json = serde_json::to_string(&task.plans).map_err(|_| Error::Internal)?;
        let confirmations_json =
            serde_json::to_string(&task.risk_confirmations).map_err(|_| Error::Internal)?;
        let revision_matches: bool = self.db()?.query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM task_configuration_revisions
                  WHERE task_id=?1 AND revision=?2
                    AND name=?3 AND source_id=?4 AND sink_id=?5
                    AND source_database=?6 AND sink_database=?7
                    AND source_revision=?8 AND sink_revision=?9
                    AND start_mode=?10 AND mappings_json=?11
                    AND plan_version IS ?12 AND plan_status=?13
                    AND plan_invalid_reason IS ?14
                    AND source_metadata_fingerprint IS ?15
                    AND sink_metadata_fingerprint IS ?16
                    AND connector_summary_json=?17
                    AND capability_summary_json=?18
                    AND capability_manifest_digest IS ?19
                    AND rule_summary_digest IS ?20
                    AND plan_set_digest IS ?21
                    AND plans_json=?22
                    AND risk_confirmations_json=?23
             )",
            params![
                task.id,
                task.desired_configuration_revision,
                task.name,
                task.source_id,
                task.sink_id,
                task.source_database,
                task.sink_database,
                task.source_revision,
                task.sink_revision,
                task.start_mode,
                mappings_json,
                task.plan_version.as_deref(),
                task.plan_status,
                task.plan_invalid_reason.as_deref(),
                task.source_metadata_fingerprint.as_deref(),
                task.sink_metadata_fingerprint.as_deref(),
                task.connector_summary_json,
                task.capability_summary_json,
                task.capability_manifest_digest.as_deref(),
                task.rule_summary_digest.as_deref(),
                task.plan_set_digest.as_deref(),
                plans_json,
                confirmations_json,
            ],
            |row| row.get(0),
        )?;
        if !revision_matches {
            return Err(Error::Conflict(
                "任务配置 revision 不存在或内容已损坏，请先重新预检",
            ));
        }
        let persisted_plan_rows: Vec<(i64, String, String, String, String)> = {
            let conn = self.db()?;
            let mut statement = conn.prepare(
                "SELECT ordinal,source_field_lineage,target_field_lineage,plan_digest,plan_json
                 FROM task_conversion_plans WHERE task_id=?1 ORDER BY ordinal",
            )?;
            statement
                .query_map([&task.id], |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                })?
                .collect::<rusqlite::Result<_>>()?
        };
        let plan_rows_match = persisted_plan_rows.len() == task.plans.len()
            && persisted_plan_rows.iter().zip(&task.plans).enumerate().all(
                |(ordinal, (row, plan))| {
                    row.0 == ordinal as i64
                        && row.1 == plan.source_field.lineage_id
                        && row.2 == plan.target_field.lineage_id
                        && row.3 == plan.plan_digest
                        && serde_json::from_str::<change_event::ColumnConversionPlan>(&row.4)
                            .map(|stored| stored == *plan)
                            .unwrap_or(false)
                },
            );
        if !plan_rows_match {
            return Err(Error::Conflict(
                "任务保存的 ColumnConversionPlan 明细不完整或已损坏，请先重新预检",
            ));
        }
        if task.plans.iter().any(|plan| {
            !plan.verify_digest()
                || (plan.confirmation == change_event::PlanConfirmationState::Required)
        }) {
            return Err(Error::Conflict(
                "任务的 ColumnConversionPlan 不完整或缺少风险确认，请先重新预检",
            ));
        }
        let input = input_from_task(task);
        let current = match self.preflight_task_with_snapshot(
            actor,
            &input,
            &task.id,
            task.configuration_revision,
        ) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                self.mark_plan_stale(&task.id, &error.to_string())?;
                return Err(Error::Conflict(
                    "任务元数据或能力输入已变化，请重新预检并确认",
                ));
            }
        };
        let matches = task.source_metadata_fingerprint.as_deref()
            == Some(current.source_metadata_fingerprint.as_str())
            && task.sink_metadata_fingerprint.as_deref()
                == Some(current.sink_metadata_fingerprint.as_str())
            && task.capability_manifest_digest.as_deref()
                == Some(current.capability_manifest_digest.as_str())
            && task.rule_summary_digest.as_deref() == Some(current.rule_summary_digest.as_str())
            && task.plan_set_digest.as_deref() == Some(current.plan_set_digest.as_str())
            && task.connector_summary_json == current.connector_summary_json
            && task.capability_summary_json == current.capability_summary_json
            && task.plans == current.plans;
        if !matches {
            self.mark_plan_stale(&task.id, "源/目的元数据、连接器、能力清单或转换规则已变化")?;
            return Err(Error::Conflict(
                "任务元数据或能力输入已变化，请重新预检并确认",
            ));
        }
        Ok(())
    }
}

impl Store {
    pub(crate) fn connector(
        &self,
        id: &str,
        role: EndpointRole,
    ) -> Result<&'static crate::registry::ConnectorDescriptor> {
        let conn = self.db()?;
        let (kind, version): (String, String) = conn
            .query_row(
                "SELECT kind,version FROM instances WHERE id=?1",
                [id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?
            .ok_or(Error::NotFound)?;
        let connector = match role {
            EndpointRole::Source => SourceRegistry.find(&kind, &version),
            EndpointRole::Sink => SinkRegistry.find(&kind, &version),
        };
        connector.ok_or(Error::Invalid("Web 未注册该数据库连接器"))
    }

    fn connector_in_transaction(
        &self,
        conn: &rusqlite::Transaction<'_>,
        id: &str,
        role: EndpointRole,
    ) -> Result<&'static crate::registry::ConnectorDescriptor> {
        let (kind, version): (String, String) = conn
            .query_row(
                "SELECT kind,version FROM instances WHERE id=?1",
                [id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?
            .ok_or(Error::NotFound)?;
        let connector = match role {
            EndpointRole::Source => SourceRegistry.find(&kind, &version),
            EndpointRole::Sink => SinkRegistry.find(&kind, &version),
        };
        connector.ok_or(Error::Invalid("Web 未注册该数据库连接器"))
    }
}
