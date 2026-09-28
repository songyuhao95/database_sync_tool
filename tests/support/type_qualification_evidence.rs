//! Writes machine-readable receipts only after a test has verified each listed
//! native declaration through planning, capture, ChangeEvent replay, and DML.

use serde_json::json;
use std::{
    collections::BTreeSet,
    env, fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

#[allow(dead_code)]
pub fn record_source_type_evidence(
    connector_id: &str,
    suite_id: &str,
    type_ids: impl IntoIterator<Item = String>,
    representation_type_ids: impl IntoIterator<Item = String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let Some(directory) = env::var_os("CDC_TYPE_QUALIFICATION_ARTIFACT_DIR") else {
        return Ok(());
    };
    let directory = PathBuf::from(directory);
    fs::create_dir_all(&directory)?;

    let root = workspace_root()?;
    let artifact_directory = directory.canonicalize()?;
    let relative_directory = artifact_directory.strip_prefix(&root).map_err(|_| {
        std::io::Error::other("type qualification evidence directory must be within the workspace")
    })?;
    let artifact_path = relative_directory
        .components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/");
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let run_id = format!("{connector_id}-{timestamp}");
    let representation_types = representation_type_ids.into_iter().collect::<BTreeSet<_>>();
    let mut records = Vec::new();
    for type_id in type_ids.into_iter().collect::<BTreeSet<_>>() {
        for axis in [
            "source.protocol_capture",
            "source.semantic_codec",
            "source.change_event",
            "source.live",
        ]
        .into_iter()
        .chain(
            representation_types
                .contains(&type_id)
                .then_some([
                    "source.source_representation_capture",
                    "source.protocol_framing",
                ])
                .into_iter()
                .flatten(),
        ) {
            records.push(json!({
                "id": format!("{type_id}@{connector_id}:{axis}:{run_id}"),
                "type_id": type_id.clone(),
                "source_connector_id": connector_id,
                "sink_connector_id": null,
                "axis": axis,
                "status": "PASS",
                "run_id": run_id
            }));
        }
    }
    if records.is_empty() {
        return Err(
            std::io::Error::other("live source test matched no native type declarations").into(),
        );
    }

    let file_name = format!("{run_id}.json");
    let relative_artifact_path = format!("{artifact_path}/{file_name}");
    let artifact_file = directory.join(file_name);
    let report = json!({
        "schema": "cdc.type_qualification_evidence_report.v1",
        "run_id": run_id,
        "suite_id": suite_id,
        "artifact_path": relative_artifact_path,
        "assertions": [
            "native declaration was present in the isolated live fixture",
            "source protocol value decoded to its checked logical representation",
            "committed INSERT, UPDATE and DELETE were captured as ChangeEvent transactions",
            "row-image presence and ChangeEvent JSON replay were verified"
        ],
        "evidence": records
    });
    fs::write(&artifact_file, serde_json::to_vec_pretty(&report)?)?;
    println!(
        "TYPE_EVIDENCE {}: {} source declarations",
        suite_id,
        report["evidence"].as_array().map_or(0, Vec::len)
    );
    Ok(())
}

#[allow(dead_code)]
pub fn record_sink_type_evidence(
    source_connector_id: &str,
    sink_connector_id: &str,
    suite_id: &str,
    type_ids: impl IntoIterator<Item = String>,
    outcome: &str,
    target_storage_mode: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let Some(directory) = env::var_os("CDC_TYPE_QUALIFICATION_ARTIFACT_DIR") else {
        return Ok(());
    };
    let directory = PathBuf::from(directory);
    fs::create_dir_all(&directory)?;

    let root = workspace_root()?;
    let artifact_directory = directory.canonicalize()?;
    let relative_directory = artifact_directory.strip_prefix(&root).map_err(|_| {
        std::io::Error::other("type qualification evidence directory must be within the workspace")
    })?;
    let artifact_path = relative_directory
        .components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/");
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let run_id = format!("{source_connector_id}-to-{sink_connector_id}-{timestamp}");
    let mode_matches_outcome = matches!(
        (outcome, target_storage_mode),
        (
            "VALUE_PRESERVED",
            "native_target_column" | "logical_value_json_carrier"
        ) | (
            "SOURCE_REPRESENTATION_PRESERVED",
            "source_representation_blob_carrier"
        )
    );
    if !mode_matches_outcome {
        return Err(std::io::Error::other(format!(
            "sink outcome {outcome} is incompatible with target storage mode {target_storage_mode}"
        ))
        .into());
    }
    let mut records = Vec::new();
    for type_id in type_ids.into_iter().collect::<BTreeSet<_>>() {
        records.push(json!({
            "id": format!("{type_id}@{source_connector_id}>{sink_connector_id}:sink.offline:{run_id}"),
            "type_id": type_id.clone(),
            "source_connector_id": source_connector_id,
            "sink_connector_id": sink_connector_id,
            "axis": "sink.offline",
            "status": "PASS",
            "run_id": run_id,
            "outcome": outcome,
            "target_storage_mode": target_storage_mode
        }));
        records.push(json!({
            "id": format!("{type_id}@{source_connector_id}>{sink_connector_id}:sink.live:{run_id}"),
            "type_id": type_id.clone(),
            "source_connector_id": source_connector_id,
            "sink_connector_id": sink_connector_id,
            "axis": "sink.live",
            "status": "PASS",
            "run_id": run_id,
            "outcome": outcome,
            "target_storage_mode": target_storage_mode
        }));
        if outcome == "SOURCE_REPRESENTATION_PRESERVED" {
            for axis in [
                "sink.representation_carrier",
                "sink.representation_preserved",
            ] {
                records.push(json!({
                    "id": format!("{type_id}@{source_connector_id}>{sink_connector_id}:{axis}:{run_id}"),
                    "type_id": type_id.clone(),
                    "source_connector_id": source_connector_id,
                    "sink_connector_id": sink_connector_id,
                    "axis": axis,
                    "status": "PASS",
                    "run_id": run_id,
                    "outcome": outcome
                }));
            }
        }
    }
    if records.is_empty() {
        return Err(
            std::io::Error::other("live sink test matched no native type declarations").into(),
        );
    }

    let file_name = format!("{run_id}.json");
    let relative_artifact_path = format!("{artifact_path}/{file_name}");
    let artifact_file = directory.join(file_name);
    let report = json!({
        "schema": "cdc.type_qualification_evidence_report.v1",
        "run_id": run_id,
        "suite_id": suite_id,
        "artifact_path": relative_artifact_path,
        "assertions": [
            "the shared compatibility planner produced a plan for each listed native declaration",
            "risk-required plans were explicitly confirmed before SQL generation",
            "the sink adapter generated DML SQL for the captured transaction",
            "the source ChangeEvent transaction was executed by the configured live sink adapter",
            "every listed native column was read back from the target and compared with its expected value",
            "source representation receipts additionally require envelope integrity validation and readback comparison",
            "the source transaction commit completed before evidence was recorded"
        ],
        "evidence": records
    });
    fs::write(&artifact_file, serde_json::to_vec_pretty(&report)?)?;
    println!(
        "TYPE_EVIDENCE {suite_id}: {} source-native declarations written and read back by {sink_connector_id}",
        report["evidence"].as_array().map_or(0, Vec::len)
    );
    Ok(())
}

#[allow(dead_code)]
pub fn record_web_plan_evidence(
    source_connector_id: &str,
    sink_connector_id: &str,
    suite_id: &str,
    type_id: &str,
    plan: &change_event::ColumnConversionPlan,
    target_storage_mode: &str,
    risk_confirmation_verified: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let Some(directory) = env::var_os("CDC_TYPE_QUALIFICATION_ARTIFACT_DIR") else {
        return Ok(());
    };
    assert!(
        plan.verify_digest(),
        "Web plan evidence requires a valid digest"
    );
    assert!(
        matches!(
            target_storage_mode,
            "native_target_column"
                | "logical_value_json_carrier"
                | "source_representation_blob_carrier"
        ),
        "Web evidence needs a known target storage mode"
    );
    let target_probe_digest = plan
        .target_probe_digest
        .as_deref()
        .filter(|digest| digest.len() == 64)
        .ok_or_else(|| std::io::Error::other("Web plan lacks a target probe digest"))?;
    assert_ne!(
        plan.confirmation,
        change_event::PlanConfirmationState::Required,
        "unconfirmed Web plans cannot produce qualification evidence"
    );
    let risk_confirmation_required =
        plan.confirmation == change_event::PlanConfirmationState::Confirmed;
    assert!(
        !risk_confirmation_required || risk_confirmation_verified,
        "risk-required Web plans need verified confirmation"
    );
    let directory = PathBuf::from(directory);
    fs::create_dir_all(&directory)?;
    let root = workspace_root()?;
    let artifact_directory = directory.canonicalize()?;
    let relative_directory = artifact_directory.strip_prefix(&root).map_err(|_| {
        std::io::Error::other("Web evidence directory must be within the workspace")
    })?;
    let artifact_path = relative_directory
        .components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/");
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let run_id = format!("{source_connector_id}-to-{sink_connector_id}-web-{timestamp}");
    let file_name = format!("{run_id}.json");
    let relative_artifact_path = format!("{artifact_path}/{file_name}");
    let report = json!({
        "schema": "cdc.type_qualification_evidence_report.v1",
        "run_id": run_id,
        "suite_id": suite_id,
        "artifact_path": relative_artifact_path,
        "assertions": [
            "the real Web preview listed the probed target representation",
            "the user selected a rule and, when required, confirmed its risk",
            "the saved task carried the same plan digest and confirmation",
            "the task passed its start gate and executed incremental DML with target readback"
        ],
        "evidence": [{
            "id": format!("{type_id}@{source_connector_id}>{sink_connector_id}:web.plan:{run_id}"),
            "type_id": type_id,
            "source_connector_id": source_connector_id,
            "sink_connector_id": sink_connector_id,
            "axis": "web.plan",
            "status": "PASS",
            "run_id": run_id,
            "verification": "web_ui.preview_save_start_gate",
            "preview_status": "COMPATIBLE",
            "save_status": "PASS",
            "start_gate_status": "PASS",
            "plan_digest": plan.plan_digest,
            "target_probe_digest": target_probe_digest,
            "selected_rule_id": plan.rule.id,
            "target_storage_mode": target_storage_mode,
            "risk_confirmation_required": risk_confirmation_required,
            "risk_confirmation_verified": risk_confirmation_verified
        }]
    });
    fs::write(
        directory.join(file_name),
        serde_json::to_vec_pretty(&report)?,
    )?;
    Ok(())
}

#[allow(dead_code)]
pub fn record_dynamic_type_class_evidence(
    connector_id: &str,
    suite_id: &str,
    class_ids: impl IntoIterator<Item = String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let Some(directory) = env::var_os("CDC_TYPE_QUALIFICATION_ARTIFACT_DIR") else {
        return Ok(());
    };
    let directory = PathBuf::from(directory);
    fs::create_dir_all(&directory)?;

    let root = workspace_root()?;
    let artifact_directory = directory.canonicalize()?;
    let relative_directory = artifact_directory.strip_prefix(&root).map_err(|_| {
        std::io::Error::other("type qualification evidence directory must be within the workspace")
    })?;
    let artifact_path = relative_directory
        .components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/");
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let run_id = format!("{connector_id}-dynamic-types-{timestamp}");
    let records = class_ids
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .flat_map(|class_id| {
            let type_id = format!("dynamic:{class_id}");
            let run_id = run_id.clone();
            [
                "source.dynamic_type_class_fixture",
                "source.protocol_capture",
                "source.semantic_codec",
                "source.change_event",
                "source.live",
            ]
            .into_iter()
            .map(move |axis| {
                json!({
                    "id": format!("{type_id}@{connector_id}:{axis}:{run_id}"),
                    "type_id": type_id.clone(),
                    "source_connector_id": connector_id,
                    "sink_connector_id": null,
                    "axis": axis,
                    "status": "PASS",
                    "run_id": run_id.clone()
                })
            })
        })
        .collect::<Vec<_>>();
    if records.is_empty() {
        return Err(
            std::io::Error::other("dynamic type fixture matched no catalog classes").into(),
        );
    }

    let file_name = format!("{run_id}.json");
    let relative_artifact_path = format!("{artifact_path}/{file_name}");
    let artifact_file = directory.join(file_name);
    let report = json!({
        "schema": "cdc.type_qualification_evidence_report.v1",
        "run_id": run_id,
        "suite_id": suite_id,
        "artifact_path": relative_artifact_path,
        "assertions": [
            "the live catalog exposed the declared dynamic type class",
            "the isolated fixture used a concrete catalog-defined type from the class",
            "the source adapter captured INSERT, UPDATE and DELETE as validated ChangeEvent transactions",
            "the class-specific logical value or source representation was checked after JSON replay",
            "protocol capture, semantic decoding, ChangeEvent framing, and live source evidence all refer to the same verified fixture"
        ],
        "evidence": records
    });
    fs::write(&artifact_file, serde_json::to_vec_pretty(&report)?)?;
    println!(
        "TYPE_EVIDENCE {suite_id}: {} dynamic PostgreSQL catalog classes exercised",
        report["evidence"].as_array().map_or(0, Vec::len)
    );
    Ok(())
}

#[allow(dead_code)]
pub fn record_catalog_type_mapping_evidence(
    connector_id: &str,
    class_id: &str,
    suite_id: &str,
    catalog_type_count: usize,
    excluded_pseudotype_array_count: usize,
    catalog_scope: &str,
    catalog_digest: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let Some(directory) = env::var_os("CDC_TYPE_QUALIFICATION_ARTIFACT_DIR") else {
        return Ok(());
    };
    let directory = PathBuf::from(directory);
    fs::create_dir_all(&directory)?;

    let root = workspace_root()?;
    let artifact_directory = directory.canonicalize()?;
    let relative_directory = artifact_directory.strip_prefix(&root).map_err(|_| {
        std::io::Error::other("type qualification evidence directory must be within the workspace")
    })?;
    let artifact_path = relative_directory
        .components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/");
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let run_id = format!("{connector_id}-catalog-mapping-{timestamp}");
    let report = json!({
        "schema": "cdc.type_qualification_evidence_report.v1",
        "run_id": run_id,
        "suite_id": suite_id,
        "artifact_path": format!("{artifact_path}/{run_id}.json"),
        "assertions": [
            "every supported stored pg_type definition in the live catalog resolved to a SourceTypeMapping",
            "every unhandled catalog type failed the suite instead of being omitted",
            "array types whose element is a PostgreSQL pseudotype were counted separately as non-storable exclusions",
            "the complete version-local catalog snapshot is bound by its stable digest"
        ],
        "catalog_type_count": catalog_type_count,
        "excluded_pseudotype_array_count": excluded_pseudotype_array_count,
        "catalog_scope": catalog_scope,
        "catalog_digest": catalog_digest,
        "evidence": [{
            "id": format!("dynamic:{class_id}@{connector_id}:source.catalog_type_mapping:{run_id}"),
            "type_id": format!("dynamic:{class_id}"),
            "source_connector_id": connector_id,
            "sink_connector_id": null,
            "axis": "source.catalog_type_mapping",
            "status": "PASS",
            "run_id": run_id,
            "catalog_type_count": catalog_type_count,
            "excluded_pseudotype_array_count": excluded_pseudotype_array_count,
            "catalog_digest": catalog_digest
        }]
    });
    fs::write(
        directory.join(format!("{run_id}.json")),
        serde_json::to_vec_pretty(&report)?,
    )?;
    println!(
        "TYPE_EVIDENCE {suite_id}: {catalog_type_count} live catalog types mapped; {excluded_pseudotype_array_count} arrays of excluded pseudotypes"
    );
    Ok(())
}

#[allow(dead_code)]
pub fn source_native_type_ids(
    connector_id: &str,
    declarations: impl IntoIterator<Item = String>,
) -> Vec<String> {
    let declarations = declarations
        .into_iter()
        .map(|declaration| normalize_declaration(&declaration))
        .collect::<BTreeSet<_>>();
    let inventory: serde_json::Value =
        serde_json::from_str(include_str!("../../scripts/type-inventory.json"))
            .expect("native type inventory is valid JSON");
    inventory["types"]
        .as_array()
        .expect("native type list")
        .iter()
        .filter_map(|entry| {
            let profile_id = entry["declaration_profile"].as_str()?;
            let profile = &inventory["native_declaration_profiles"][profile_id];
            let applies_to_connector = profile["connectors"]
                .as_array()?
                .iter()
                .any(|id| id.as_str() == Some(connector_id));
            let matches_fixture = profile["examples"].as_array()?.iter().any(|example| {
                example
                    .as_str()
                    .is_some_and(|example| declarations.contains(&normalize_declaration(example)))
            });
            (applies_to_connector && matches_fixture)
                .then(|| entry["id"].as_str().map(str::to_owned))
                .flatten()
        })
        .collect()
}

#[allow(dead_code)]
fn normalize_declaration(declaration: &str) -> String {
    declaration
        .trim()
        .to_ascii_lowercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

pub fn workspace_root() -> Result<PathBuf, std::io::Error> {
    let mut root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    while !root.join("scripts/type-inventory.json").is_file() {
        if !root.pop() {
            return Err(std::io::Error::other(
                "could not locate workspace type inventory",
            ));
        }
    }
    root.canonicalize()
}
