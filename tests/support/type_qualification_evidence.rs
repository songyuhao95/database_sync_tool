//! Writes machine-readable receipts only after a test has verified each listed
//! native declaration through planning, capture, ChangeEvent replay, and DML.

use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet},
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
        let mut axes = vec![
            "source.protocol_capture",
            "source.change_event",
            "source.live",
        ];
        if representation_types.contains(&type_id) {
            axes.extend([
                "source.source_representation_capture",
                "source.protocol_framing",
            ]);
        } else {
            axes.push("source.semantic_codec");
        }
        for axis in axes {
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
pub fn record_source_null_only_type_evidence(
    connector_id: &str,
    suite_id: &str,
    type_ids: impl IntoIterator<Item = String>,
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
    let run_id = format!("{connector_id}-null-only-{timestamp}");
    let type_ids = type_ids.into_iter().collect::<BTreeSet<_>>();
    if type_ids.is_empty() {
        return Ok(());
    }
    let records = type_ids
        .into_iter()
        .flat_map(|type_id| {
            let connector_id = connector_id.to_owned();
            let run_id = run_id.clone();
            [
                "source.protocol_capture",
                "source.change_event",
                "source.live",
                "source.null_only",
            ]
            .into_iter()
            .map(move |axis| {
                json!({
                    "id": format!("{type_id}@{connector_id}:{axis}:{run_id}"),
                    "type_id": type_id,
                    "source_connector_id": connector_id,
                    "sink_connector_id": null,
                    "axis": axis,
                    "status": "PASS",
                    "value_coverage": "NULL_ONLY",
                    "run_id": run_id
                })
            })
        })
        .collect::<Vec<_>>();
    if records.is_empty() {
        return Err(std::io::Error::other("null-only source fixture qualified no types").into());
    }
    let file_name = format!("{run_id}.json");
    let report = json!({
        "schema": "cdc.type_qualification_evidence_report.v1",
        "run_id": run_id,
        "suite_id": suite_id,
        "artifact_path": format!("{artifact_path}/{file_name}"),
        "assertions": [
            "the exact catalog-defined column type was present in a live DML fixture",
            "a SQL NULL value was captured and retained through ChangeEvent replay",
            "the source type is explicitly classified as NULL-only; no non-NULL SQL value or semantic/representation codec claim is made"
        ],
        "evidence": records
    });
    fs::write(
        directory.join(file_name),
        serde_json::to_vec_pretty(&report)?,
    )?;
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
            "source_representation_blob_carrier" | "logical_value_json_carrier"
        ) | (
            "NULL_PRESERVED",
            "native_target_column"
                | "logical_value_json_carrier"
                | "source_representation_blob_carrier"
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
            "target_storage_mode": target_storage_mode,
            "target_readback_status": "PASS"
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
            "every listed native column was read back from the target according to the receipt outcome and compared with its expected value or explicit NULL",
            "NULL_PRESERVED receipts apply only to explicitly NULL_ONLY source types and do not claim general non-NULL value preservation",
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
pub fn record_sink_live_type_evidence(
    source_connector_id: &str,
    sink_connector_id: &str,
    suite_id: &str,
    qualified_types: impl IntoIterator<Item = (String, String, String)>,
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
    let run_id = format!("{source_connector_id}-to-{sink_connector_id}-readback-{timestamp}");
    let mut records = Vec::new();
    for (type_id, outcome, target_storage_mode) in qualified_types {
        let expected = matches!(
            (outcome.as_str(), target_storage_mode.as_str()),
            (
                "VALUE_PRESERVED",
                "native_target_column" | "logical_value_json_carrier"
            ) | (
                "SOURCE_REPRESENTATION_PRESERVED",
                "source_representation_blob_carrier"
            ) | (
                "NULL_PRESERVED",
                "native_target_column"
                    | "logical_value_json_carrier"
                    | "source_representation_blob_carrier"
            )
        );
        if !expected {
            return Err(std::io::Error::other(format!(
                "sink outcome {outcome} is incompatible with target storage mode {target_storage_mode}"
            )).into());
        }
        records.push(json!({
            "id": format!("{type_id}@{source_connector_id}>{sink_connector_id}:sink.live:{run_id}"),
            "type_id": type_id,
            "source_connector_id": source_connector_id,
            "sink_connector_id": sink_connector_id,
            "axis": "sink.live",
            "status": "PASS",
            "run_id": run_id,
            "outcome": outcome,
            "target_storage_mode": target_storage_mode,
            "target_readback_status": "PASS"
        }));
        if outcome == "SOURCE_REPRESENTATION_PRESERVED" {
            for axis in [
                "sink.representation_carrier",
                "sink.representation_preserved",
            ] {
                records.push(json!({
                    "id": format!("{type_id}@{source_connector_id}>{sink_connector_id}:{axis}:{run_id}"),
                    "type_id": type_id,
                    "source_connector_id": source_connector_id,
                    "sink_connector_id": sink_connector_id,
                    "axis": axis,
                    "status": "PASS",
                    "run_id": run_id,
                    "outcome": outcome,
                    "target_storage_mode": target_storage_mode,
                    "target_readback_status": "PASS"
                }));
            }
        }
    }
    if records.is_empty() {
        return Err(std::io::Error::other(
            "live sink readback qualified no catalog type instances",
        )
        .into());
    }
    let file_name = format!("{run_id}.json");
    let report = json!({
        "schema": "cdc.type_qualification_evidence_report.v1",
        "run_id": run_id,
        "suite_id": suite_id,
        "artifact_path": format!("{artifact_path}/{file_name}"),
        "assertions": [
            "a live CDC task applied each exact catalog type instance",
            "the target value was compared to the source ChangeEvent value or verified source-representation envelope",
            "the receipt records live target readback only; it makes no separate offline qualification claim"
        ],
        "evidence": records
    });
    fs::write(
        directory.join(file_name),
        serde_json::to_vec_pretty(&report)?,
    )?;
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
            let mut axes = vec![
                "source.dynamic_type_class_fixture",
                "source.protocol_capture",
                "source.change_event",
                "source.live",
            ];
            if class_id == "postgresql.other_defined_catalog_types" {
                axes.push("source.source_representation_capture");
            } else {
                axes.push("source.semantic_codec");
            }
            axes.into_iter().map(move |axis| {
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
pub fn record_dynamic_type_class_sink_evidence(
    source_connector_id: &str,
    sink_connector_id: &str,
    suite_id: &str,
    class_ids: impl IntoIterator<Item = String>,
    outcome: &str,
    target_storage_mode: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if outcome != "SOURCE_REPRESENTATION_PRESERVED"
        || target_storage_mode != "source_representation_blob_carrier"
    {
        return Err(std::io::Error::other(
            "dynamic catalog fallback evidence requires a read-back-verified source representation carrier",
        )
        .into());
    }
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
    let run_id = format!("{source_connector_id}-to-{sink_connector_id}-dynamic-{timestamp}");
    let records = class_ids
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .flat_map(|class_id| {
            let type_id = format!("dynamic:{class_id}");
            let run_id = run_id.clone();
            ["sink.offline", "sink.live", "sink.representation_carrier", "sink.representation_preserved"]
                .into_iter()
                .map(move |axis| {
                    json!({
                        "id": format!("{type_id}@{source_connector_id}>{sink_connector_id}:{axis}:{run_id}"),
                        "type_id": type_id.clone(),
                        "source_connector_id": source_connector_id,
                        "sink_connector_id": sink_connector_id,
                        "axis": axis,
                        "status": "PASS",
                        "run_id": run_id.clone(),
                        "outcome": outcome,
                        "target_storage_mode": target_storage_mode
                    })
                })
        })
        .collect::<Vec<_>>();
    if records.is_empty() {
        return Err(std::io::Error::other("dynamic sink fixture matched no type classes").into());
    }
    let file_name = format!("{run_id}.json");
    let report = json!({
        "schema": "cdc.type_qualification_evidence_report.v1",
        "run_id": run_id,
        "suite_id": suite_id,
        "artifact_path": format!("{artifact_path}/{file_name}"),
        "assertions": [
            "the same captured catalog-defined source representation was applied by the live Sink",
            "the precreated carrier returned the exact validated envelope including payload digest and type context"
        ],
        "evidence": records
    });
    fs::write(
        directory.join(file_name),
        serde_json::to_vec_pretty(&report)?,
    )?;
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
            "the live visible-column catalog scan bound all observed MySQL type mapping profiles by one stable digest"
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
    Ok(())
}

#[allow(dead_code)]
pub struct ExactCatalogTypeMappingEvidence {
    pub connector_id: String,
    pub suite_id: String,
    pub catalog_type_count: usize,
    pub excluded_pseudotype_array_count: usize,
    pub catalog_scope: String,
    pub catalog_digest: String,
    pub type_roster: Vec<serde_json::Value>,
    pub non_storable_type_count: usize,
}

#[allow(dead_code)]
pub fn record_exact_catalog_type_mapping_evidence(
    evidence: ExactCatalogTypeMappingEvidence,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let ExactCatalogTypeMappingEvidence {
        connector_id,
        suite_id,
        catalog_type_count,
        excluded_pseudotype_array_count,
        catalog_scope,
        catalog_digest,
        type_roster,
        non_storable_type_count,
    } = evidence;
    let connector_id = connector_id.as_str();
    let suite_id = suite_id.as_str();
    let catalog_scope = catalog_scope.as_str();
    let catalog_digest = catalog_digest.as_str();
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
    if type_roster.len() != catalog_type_count {
        return Err(std::io::Error::other(format!(
            "catalog mapping roster has {} entries for {catalog_type_count} mapped types",
            type_roster.len()
        ))
        .into());
    }
    if type_roster
        .iter()
        .filter(|definition| definition["user_storable"] == false)
        .count()
        != non_storable_type_count
    {
        return Err(std::io::Error::other(
            "catalog mapping roster non-storable count does not match its declared count",
        )
        .into());
    }
    let mut records = Vec::with_capacity(type_roster.len());
    let mut seen_type_ids = std::collections::BTreeSet::new();
    for definition in &type_roster {
        let type_id = definition["type_id"]
            .as_str()
            .ok_or_else(|| std::io::Error::other("catalog type mapping has no type_id"))?;
        let schema = definition["schema"]
            .as_str()
            .ok_or_else(|| std::io::Error::other("catalog type mapping has no schema"))?;
        let name = definition["name"]
            .as_str()
            .ok_or_else(|| std::io::Error::other("catalog type mapping has no name"))?;
        let definition_digest = definition["definition_digest"].as_str().ok_or_else(|| {
            std::io::Error::other("catalog type mapping has no definition digest")
        })?;
        if schema.is_empty()
            || name.is_empty()
            || !seen_type_ids.insert(type_id)
            || definition_digest.len() != 64
            || !definition_digest
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
            || definition["catalog_class_id"].as_str().is_none()
            || definition["mapping_id"].as_str().is_none()
            || definition["logical_type_digest"].as_str().is_none()
            || !definition["user_storable"].is_boolean()
            || (definition["user_storable"] == false
                && definition["non_storable_reason"]
                    .as_str()
                    .is_none_or(str::is_empty))
            || (definition["user_storable"] == true && !definition["non_storable_reason"].is_null())
        {
            return Err(
                std::io::Error::other("catalog type mapping identity is incomplete").into(),
            );
        }
        records.push(json!({
            "id": format!("{type_id}@{connector_id}:source.catalog_type_mapping:{run_id}"),
            "type_id": type_id,
            "source_connector_id": connector_id,
            "sink_connector_id": null,
            "axis": "source.catalog_type_mapping",
            "status": "PASS",
            "run_id": run_id,
            "catalog_class_id": definition["catalog_class_id"],
            "schema": schema,
            "name": name,
            "type_oid": definition["type_oid"],
            "native_declaration": definition["native_declaration"],
            "definition_digest": definition_digest,
            "mapping_id": definition["mapping_id"],
            "mapping_evidence_digest": definition["mapping_evidence_digest"],
            "logical_type_digest": definition["logical_type_digest"],
            "representation_mode": definition["representation_mode"],
            "user_storable": definition["user_storable"],
            "non_storable_reason": definition["non_storable_reason"],
            "catalog_type_count": catalog_type_count,
            "non_storable_type_count": non_storable_type_count,
            "excluded_pseudotype_array_count": excluded_pseudotype_array_count,
            "catalog_scope": catalog_scope,
            "catalog_digest": catalog_digest
        }));
    }
    let report = json!({
        "schema": "cdc.type_qualification_evidence_report.v1",
        "run_id": run_id,
        "suite_id": suite_id,
        "artifact_path": format!("{artifact_path}/{run_id}.json"),
        "assertions": [
            "every supported stored pg_type definition in the live catalog resolved to a SourceTypeMapping",
            "every unhandled catalog type failed the suite instead of being omitted",
            "array types whose element is a PostgreSQL pseudotype were counted separately as non-storable exclusions",
            "mapped catalog definitions that recursively depend on a PostgreSQL pseudotype remain in the exact mapping roster and are explicitly excluded from user-table DML qualification",
            "the complete version-local catalog snapshot is bound by its stable digest"
        ],
        "catalog_type_count": catalog_type_count,
        "non_storable_type_count": non_storable_type_count,
        "excluded_pseudotype_array_count": excluded_pseudotype_array_count,
        "catalog_scope": catalog_scope,
        "catalog_digest": catalog_digest,
        "catalog_inventory_mode": "per_catalog_type_instance",
        "catalog_type_roster": type_roster,
        "evidence": records
    });
    fs::write(
        directory.join(format!("{run_id}.json")),
        serde_json::to_vec_pretty(&report)?,
    )?;
    println!(
        "TYPE_EVIDENCE {suite_id}: {catalog_type_count} live catalog types mapped; {non_storable_type_count} mapped definitions non-storable; {excluded_pseudotype_array_count} arrays of excluded pseudotypes"
    );
    Ok(())
}

#[allow(dead_code)]
pub fn record_catalog_type_roster_evidence(
    connector_id: &str,
    suite_id: &str,
    type_roster: Vec<serde_json::Value>,
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
    let run_id = format!("{connector_id}-catalog-roster-{timestamp}");
    let mut roster_by_type = BTreeMap::new();
    for definition in &type_roster {
        let type_id = definition["type_id"]
            .as_str()
            .ok_or_else(|| std::io::Error::other("catalog type roster entry has no type_id"))?;
        let schema = definition["schema"]
            .as_str()
            .ok_or_else(|| std::io::Error::other("catalog type roster entry has no schema"))?;
        let name = definition["name"]
            .as_str()
            .ok_or_else(|| std::io::Error::other("catalog type roster entry has no name"))?;
        let digest = definition["definition_digest"].as_str().ok_or_else(|| {
            std::io::Error::other("catalog type roster entry has no definition digest")
        })?;
        if schema.is_empty()
            || name.is_empty()
            || digest.len() != 64
            || !digest.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(std::io::Error::other("catalog type roster identity is incomplete").into());
        }
        if let Some(previous) = roster_by_type.get(type_id) {
            if previous != definition {
                return Err(std::io::Error::other(format!(
                    "catalog type {schema}.{name} has conflicting mapping definitions in one roster"
                ))
                .into());
            }
            continue;
        }
        roster_by_type.insert(type_id, definition.clone());
    }
    let type_roster = roster_by_type.into_values().collect::<Vec<_>>();
    let mut records = Vec::with_capacity(type_roster.len());
    for definition in &type_roster {
        let type_id = definition["type_id"].as_str().unwrap();
        let schema = definition["schema"].as_str().unwrap();
        let name = definition["name"].as_str().unwrap();
        let digest = definition["definition_digest"].as_str().unwrap();
        records.push(json!({
            "id": format!("{type_id}@{connector_id}:source.catalog_type_mapping:{run_id}"),
            "type_id": type_id,
            "source_connector_id": connector_id,
            "sink_connector_id": null,
            "axis": "source.catalog_type_mapping",
            "status": "PASS",
            "run_id": run_id,
            "catalog_class_id": definition["catalog_class_id"],
            "schema": schema,
            "name": name,
            "type_oid": definition["type_oid"],
            "native_declaration": definition["native_declaration"],
            "definition_digest": digest,
            "mapping_id": definition["mapping_id"],
            "mapping_evidence_digest": definition["mapping_evidence_digest"],
            "logical_type_digest": definition["logical_type_digest"],
            "representation_mode": definition["representation_mode"]
        }));
    }
    let file_name = format!("{run_id}.json");
    let report = json!({
        "schema": "cdc.type_qualification_evidence_report.v1",
        "run_id": run_id,
        "suite_id": suite_id,
        "artifact_path": format!("{artifact_path}/{file_name}"),
        "assertions": [
            "the live server catalog enumerated each exact defined storable type identity",
            "each roster entry carries its source definition digest and versioned mapping identity",
            "pseudotypes and arrays of pseudotypes are excluded from stored-column coverage"
        ],
        "catalog_type_roster": type_roster,
        "evidence": records
    });
    fs::write(
        directory.join(file_name),
        serde_json::to_vec_pretty(&report)?,
    )?;
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
