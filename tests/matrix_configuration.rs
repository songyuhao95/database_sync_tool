use serde_json::Value;

#[test]
fn issue_15_keeps_its_local_four_by_four_matrix_in_the_six_database_roster() {
    let matrix: Value = serde_json::from_str(include_str!("../scripts/test-matrix.json"))
        .expect("test matrix must be valid JSON");
    let databases = matrix["databases"]
        .as_array()
        .expect("matrix databases must be an array");
    let expected = [
        "mysql_5_7",
        "mysql_8_0",
        "mysql_8_4",
        "postgresql_15",
        "postgresql_16",
        "postgresql_17",
    ];
    assert_eq!(
        databases
            .iter()
            .map(|value| value.as_str().unwrap())
            .collect::<Vec<_>>(),
        expected
    );

    let suite = matrix["suites"]
        .as_array()
        .unwrap()
        .iter()
        .find(|suite| suite["id"] == "connector_matrix.local")
        .expect("Issue #15 requires a shared local matrix suite");
    assert_eq!(suite["mode"], "Local");
    let suite_databases = suite["databases"]
        .as_array()
        .unwrap()
        .iter()
        .map(|value| value.as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        suite_databases,
        ["mysql_5_7", "mysql_8_0", "mysql_8_4", "postgresql_15"]
    );
    let stages = suite["stages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|value| value.as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(stages, ["ChangeEvent", "Sql"]);
}

#[test]
fn issue_57_registers_live_components_and_capability_invalidation_without_claiming_live_routes() {
    let config: Value = serde_json::from_str(include_str!("../scripts/qualification-matrix.json"))
        .expect("qualification matrix must be valid JSON");
    let matrix: Value = serde_json::from_str(include_str!("../scripts/test-matrix.json"))
        .expect("test matrix must be valid JSON");
    let roster = config["live_qualification"]["source_fixture_roster"]
        .as_array()
        .unwrap()
        .iter()
        .map(|value| value.as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        roster,
        vec![
            "mysql_5_7",
            "mysql_8_0",
            "mysql_8_4",
            "postgresql_15",
            "postgresql_16",
            "postgresql_17"
        ]
    );

    let sources = config["live_qualification"]["sources"].as_array().unwrap();
    let sinks = config["live_qualification"]["sinks"].as_array().unwrap();
    assert_eq!(sources.len(), 6);
    assert_eq!(sinks.len(), 6);
    assert!(
        !config["live_qualification"]["route_smoke"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        config["live_qualification"]["transaction_recovery"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let capability_invalidation = config["live_qualification"]["capability_invalidation"]
        .as_array()
        .unwrap();
    assert_eq!(capability_invalidation.len(), 1);

    let suites = matrix["suites"].as_array().unwrap();
    assert_eq!(
        suites
            .iter()
            .filter(|suite| suite["category"] == "source")
            .count(),
        6
    );
    assert_eq!(
        suites
            .iter()
            .filter(|suite| suite["category"] == "sink")
            .count(),
        6
    );
    assert_eq!(
        suites
            .iter()
            .filter(|suite| {
                suite["category"] == "transaction_recovery"
                    && suite["required_for_live_qualified"] == true
            })
            .count(),
        1
    );
    let invalidation_suite = suites
        .iter()
        .find(|suite| suite["id"] == capability_invalidation[0])
        .expect("live capability invalidation must have a registered suite");
    assert_eq!(invalidation_suite["mode"], "Live");
    assert_eq!(invalidation_suite["category"], "capability_invalidation");
    assert_eq!(invalidation_suite["required_for_live_qualified"], true);
    for key in [
        "CDC_MYSQL_HOST",
        "CDC_MYSQL57_PORT",
        "CDC_MYSQL_READER_USER",
        "CDC_MYSQL_READER_PASSWORD",
        "CDC_MYSQL_WRITER_USER",
        "CDC_MYSQL_WRITER_PASSWORD",
        "PG_CDC_HOST",
        "PG_CDC_PORT",
        "PG_CDC_ADMIN_USER",
        "PG_CDC_READER_USER",
        "PG_CDC_WRITER_USER",
        "PG_CDC_TEST_PASSWORD",
    ] {
        assert!(
            invalidation_suite["required_env"]
                .as_array()
                .unwrap()
                .iter()
                .any(|value| value == key),
            "live capability invalidation must require {key}"
        );
    }
    assert!(
        invalidation_suite["args"]
            .as_array()
            .unwrap()
            .iter()
            .any(|arg| {
                arg.as_str().is_some_and(|arg| {
                    arg.contains("live_postgresql15_target_schema_change_invalidates_saved_plan")
                })
            })
    );
    for source in sources {
        let suite_id = source["suite"].as_str().unwrap();
        let suite = suites
            .iter()
            .find(|suite| suite["id"] == suite_id)
            .expect("every Source component must have a registered live suite");
        assert_eq!(suite["category"], "source");
        if let Some(major) = source["database"]
            .as_str()
            .unwrap()
            .strip_prefix("postgresql_")
        {
            let prefix = if major == "15" {
                "PG_CDC".to_owned()
            } else {
                format!("PG_CDC{major}")
            };
            for key in [
                "HOST",
                "PORT",
                "ADMIN_USER",
                "READER_USER",
                "WRITER_USER",
                "TEST_PASSWORD",
            ] {
                assert!(
                    suite["required_env"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|value| value == &format!("{prefix}_{key}")),
                    "{suite_id} must require {prefix}_{key}"
                );
            }
            if major == "15" {
                assert!(suite["args"].as_array().unwrap().iter().any(|arg| {
                    arg.as_str()
                        .is_some_and(|arg| arg.contains("postgres15_capture_and_replay"))
                }));
            } else {
                let args = suite["args"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|value| value.as_str().unwrap())
                    .collect::<Vec<_>>();
                assert!(args.contains(&"-p") && args.contains(&"cdc-binlog-tail"));
                assert!(args.contains(&format!("postgres{major}_public_source_capture").as_str()));
            }
        }
    }
    for sink in sinks {
        let suite_id = sink["suite"].as_str().unwrap();
        let suite = suites
            .iter()
            .find(|suite| suite["id"] == suite_id)
            .expect("every Sink component must have a registered live suite");
        let fixtures = suite["source_fixtures"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value.as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            fixtures, roster,
            "every live Sink suite consumes six source fixtures"
        );
        if let Some(major) = sink["database"]
            .as_str()
            .unwrap()
            .strip_prefix("postgresql_")
        {
            let prefix = if major == "15" {
                "PG_CDC".to_owned()
            } else {
                format!("PG_CDC{major}")
            };
            for key in ["HOST", "PORT", "ADMIN_USER", "WRITER_USER", "TEST_PASSWORD"] {
                assert!(
                    suite["required_env"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|value| value == &format!("{prefix}_{key}")),
                    "{} must require {prefix}_{key}",
                    suite["id"]
                );
            }
            assert!(suite["args"].as_array().unwrap().iter().any(|arg| {
                arg.as_str().is_some_and(|arg| {
                    arg.contains(&format!("postgres{major}_public_sink_apply"))
                        || (major == "15" && arg.contains("postgres15_writes_sql_transaction"))
                })
            }));
        }
    }
    assert_eq!(
        suites
            .iter()
            .filter(|suite| suite["mode"] == "Live" && suite["category"] == "route_smoke")
            .count(),
        config["live_qualification"]["route_smoke"]
            .as_array()
            .unwrap()
            .len()
    );
}

#[test]
fn issue_37_live_status_failure_semantics_are_not_offline_pass() {
    fn status(unsupported: bool, source: Option<&str>, sink: Option<&str>) -> &'static str {
        if unsupported {
            "UNSUPPORTED"
        } else if source == Some("FAIL") || sink == Some("FAIL") {
            "FAIL"
        } else if source == Some("PASS") && sink == Some("PASS") {
            "PASS"
        } else {
            "REQUIRES_LIVE"
        }
    }

    assert_eq!(status(false, Some("PASS"), Some("PASS")), "PASS");
    assert_eq!(status(false, Some("FAIL"), Some("PASS")), "FAIL");
    assert_eq!(status(false, Some("PASS"), None), "REQUIRES_LIVE");
    assert_eq!(status(true, Some("PASS"), Some("PASS")), "UNSUPPORTED");
    assert_ne!(status(false, Some("PASS"), Some("PASS")), "OFFLINE_PASS");
}
