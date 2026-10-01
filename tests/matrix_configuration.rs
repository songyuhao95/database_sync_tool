use std::collections::BTreeSet;

use serde_json::Value;

#[test]
fn issue_15_keeps_its_local_four_by_four_matrix_in_the_six_database_roster() {
    let matrix: Value = serde_json::from_str(include_str!("../scripts/test-matrix.json"))
        .expect("test matrix must be valid JSON");
    let inventory: Value = serde_json::from_str(include_str!("../scripts/type-inventory.json"))
        .expect("type inventory must be valid JSON");
    let databases = inventory["connectors"]
        .as_array()
        .expect("type inventory connectors must be an array");
    let expected: Vec<_> = databases
        .iter()
        .map(|item| item["id"].as_str().unwrap())
        .collect();
    assert!(
        matrix.get("databases").is_none(),
        "type inventory owns the connector roster"
    );
    assert_eq!(expected.len(), 6);
    assert!(expected.contains(&"mysql_5_7"));
    assert!(expected.contains(&"postgresql_17"));

    let suite = matrix["suites"]
        .as_array()
        .unwrap()
        .iter()
        .find(|suite| suite["id"] == "connector_matrix.local")
        .expect("Issue #15 requires a shared local matrix suite");
    for suite in matrix["suites"].as_array().unwrap() {
        for database in suite["databases"].as_array().unwrap() {
            assert!(
                expected.contains(&database.as_str().unwrap()),
                "test suite references a connector missing from the type inventory"
            );
        }
    }
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
    let inventory: Value = serde_json::from_str(include_str!("../scripts/type-inventory.json"))
        .expect("type inventory must be valid JSON");
    let type_roster = inventory["connectors"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["id"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert!(
        config.get("databases").is_none(),
        "type inventory owns the connector roster"
    );
    assert!(
        config["live_qualification"]
            .get("source_fixture_roster")
            .is_none()
    );

    let sources = config["live_qualification"]["sources"].as_array().unwrap();
    let sinks = config["live_qualification"]["sinks"].as_array().unwrap();
    assert_eq!(sources.len(), 6);
    assert_eq!(sinks.len(), 6);
    assert_eq!(
        sources
            .iter()
            .map(|entry| entry["database"].as_str().unwrap())
            .collect::<Vec<_>>(),
        type_roster
    );
    assert_eq!(
        sinks
            .iter()
            .map(|entry| entry["database"].as_str().unwrap())
            .collect::<Vec<_>>(),
        type_roster
    );
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
    for (database, expected_suite) in [
        ("postgresql_15", "postgresql_15.representation_carriers"),
        ("postgresql_16", "postgresql_16.representation_carriers"),
        ("postgresql_17", "postgresql_17.representation_carriers"),
    ] {
        let sink = sinks
            .iter()
            .find(|sink| sink["database"] == database)
            .expect("each PostgreSQL version is a live sink component");
        assert!(
            sink["additional_suites"]
                .as_array()
                .unwrap()
                .iter()
                .any(|suite| suite == expected_suite),
            "the PostgreSQL carrier suite must gate {database} qualification"
        );
    }
    let suites = matrix["suites"].as_array().unwrap();
    for (database, suite_id, sink_host_env, sink_port_env) in [
        (
            "mysql_5_7",
            "mysql_5_7.web_same_version_all_types",
            "CDC_MYSQL57_SINK_HOST",
            "CDC_MYSQL57_SINK_PORT",
        ),
        (
            "mysql_8_0",
            "mysql_8_0.web_same_version_all_types",
            "CDC_MYSQL80_SINK_HOST",
            "CDC_MYSQL80_SINK_PORT",
        ),
        (
            "mysql_8_4",
            "mysql_8_4.web_same_version_all_types",
            "CDC_MYSQL84_SINK_HOST",
            "CDC_MYSQL84_SINK_PORT",
        ),
    ] {
        let route = suites
            .iter()
            .find(|suite| suite["id"] == suite_id)
            .unwrap_or_else(|| panic!("missing same-version Web route for {database}"));
        assert_eq!(route["category"], "route_smoke");
        assert!(
            route["required_for_live_qualified"] == true
                && route["required_env"].as_array().is_some_and(|env| {
                    env.iter().any(|name| name == sink_host_env)
                        && env.iter().any(|name| name == sink_port_env)
                }),
            "same-version Web qualification for {database} requires a separate target host and port"
        );
    }

    assert_eq!(
        suites
            .iter()
            .filter(|suite| suite["category"] == "source")
            .count(),
        19
    );
    for (database, expected_suite) in [
        ("mysql_5_7", "mysql_5_7.visible_type_catalog"),
        ("mysql_8_0", "mysql_8_0.visible_type_catalog"),
        ("mysql_8_4", "mysql_8_4.visible_type_catalog"),
    ] {
        assert!(
            suites.iter().any(|suite| suite["id"] == expected_suite
                && suite["required_for_live_qualified"] == true),
            "the visible MySQL type catalog suite must gate {database} qualification"
        );
    }
    let mysql_snapshot_suite = suites
        .iter()
        .find(|suite| suite["id"] == "mysql.all_types_snapshot_round_trip")
        .expect("all native MySQL types must have a live full-snapshot round-trip suite");
    assert_eq!(mysql_snapshot_suite["mode"], "Live");
    assert_eq!(
        mysql_snapshot_suite["databases"],
        serde_json::json!(["mysql_5_7", "mysql_8_0", "mysql_8_4"])
    );
    assert_eq!(
        mysql_snapshot_suite["stages"],
        serde_json::json!(["Read", "Sql"])
    );
    let postgres_builtin_capture_suites: BTreeSet<_> = suites
        .iter()
        .filter(|suite| {
            suite["id"]
                .as_str()
                .is_some_and(|id| id.starts_with("postgresql_") && id.ends_with(".builtins"))
        })
        .map(|suite| suite["id"].as_str().unwrap())
        .collect();
    assert_eq!(
        postgres_builtin_capture_suites,
        BTreeSet::from([
            "postgresql_15.builtins",
            "postgresql_16.builtins",
            "postgresql_17.builtins",
        ])
    );
    for suite in suites.iter().filter(|suite| {
        suite["id"]
            .as_str()
            .is_some_and(|id| id.starts_with("postgresql_") && id.ends_with(".builtins"))
    }) {
        assert_eq!(suite["mode"], "Live");
        assert_eq!(suite["category"], "source");
        assert!(
            suite["args"]
                .as_array()
                .unwrap()
                .iter()
                .any(|argument| argument == "live_builtin_capture")
        );
        assert!(suite["args"].as_array().unwrap().iter().any(|argument| {
            argument
                .as_str()
                .is_some_and(|argument| argument.ends_with("_all_builtin_types_capture"))
        }));
    }
    let postgres_recursive_capture_suites: BTreeSet<_> = suites
        .iter()
        .filter(|suite| {
            suite["id"]
                .as_str()
                .is_some_and(|id| id.starts_with("postgresql_") && id.ends_with(".recursive_types"))
        })
        .map(|suite| suite["id"].as_str().unwrap())
        .collect();
    assert_eq!(
        postgres_recursive_capture_suites,
        BTreeSet::from([
            "postgresql_15.recursive_types",
            "postgresql_16.recursive_types",
            "postgresql_17.recursive_types",
        ])
    );
    for suite in suites.iter().filter(|suite| {
        suite["id"]
            .as_str()
            .is_some_and(|id| id.starts_with("postgresql_") && id.ends_with(".recursive_types"))
    }) {
        assert_eq!(suite["mode"], "Live");
        assert_eq!(suite["category"], "source");
        assert!(
            suite["args"]
                .as_array()
                .unwrap()
                .iter()
                .any(|argument| { argument == "live_builtin_capture" })
        );
        assert!(suite["args"].as_array().unwrap().iter().any(|argument| {
            argument
                .as_str()
                .is_some_and(|argument| argument.ends_with("_recursive_type_capture"))
        }));
    }
    let all_mysql_type_capture_suites: BTreeSet<_> = suites
        .iter()
        .filter(|suite| {
            suite["id"]
                .as_str()
                .is_some_and(|id| id.ends_with(".all_types_capture"))
        })
        .map(|suite| suite["id"].as_str().unwrap())
        .collect();
    assert_eq!(
        all_mysql_type_capture_suites,
        BTreeSet::from([
            "mysql_5_7.all_types_capture",
            "mysql_8_0.all_types_capture",
            "mysql_8_4.all_types_capture",
        ])
    );
    assert_eq!(
        suites
            .iter()
            .filter(|suite| suite["category"] == "sink")
            .count(),
        15
    );
    for suite_id in [
        "mysql_5_7.representation_carriers",
        "mysql_8_0.representation_carriers",
        "mysql_8_4.representation_carriers",
    ] {
        let suite = suites
            .iter()
            .find(|suite| suite["id"] == suite_id)
            .unwrap_or_else(|| panic!("representation read-back suite {suite_id} is registered"));
        assert_eq!(suite["mode"], "Live");
        assert_eq!(suite["category"], "sink");
        assert!(suite["args"].as_array().unwrap().iter().any(|argument| {
            argument.as_str().is_some_and(|argument| {
                argument.ends_with("_live_representation_readback_and_checkpoint")
            })
        }));
    }
    for suite_id in [
        "postgresql_15.representation_carriers",
        "postgresql_16.representation_carriers",
        "postgresql_17.representation_carriers",
    ] {
        let suite = suites
            .iter()
            .find(|suite| suite["id"] == suite_id)
            .unwrap_or_else(|| panic!("PostgreSQL carrier suite {suite_id} is registered"));
        assert_eq!(suite["mode"], "Live");
        assert_eq!(suite["category"], "sink");
        assert_eq!(suite["required_for_live_qualified"], true);
        assert_eq!(
            suite["source_fixtures"],
            serde_json::json!(["postgresql_15"])
        );
        assert!(suite["args"].as_array().unwrap().iter().any(|argument| {
            argument.as_str().is_some_and(|argument| {
                argument.ends_with("_sink_representation_carriers_live_qualification")
            })
        }));
    }
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
        let database = source["database"].as_str().unwrap();
        let expected_type_suites: Vec<String> = match database {
            "mysql_5_7" | "mysql_8_0" | "mysql_8_4" => {
                vec![format!("{database}.all_types_capture")]
            }
            "postgresql_15" | "postgresql_16" | "postgresql_17" => vec![
                format!("{database}.builtins"),
                format!("{database}.recursive_types"),
            ],
            _ => unreachable!("six connector roster is fixed"),
        };
        let additional = source["additional_suites"].as_array().unwrap();
        assert_eq!(
            additional
                .iter()
                .map(|value| value.as_str().unwrap().to_owned())
                .collect::<Vec<_>>(),
            expected_type_suites,
            "every Source qualification must require its all-native-type fixtures"
        );
        for additional_suite in additional {
            let id = additional_suite.as_str().unwrap();
            let definition = suites
                .iter()
                .find(|candidate| candidate["id"] == id)
                .expect("all-native-type Source fixture must be registered");
            assert_eq!(definition["category"], "source");
            assert_eq!(definition["required_for_live_qualified"], true);
            assert!(
                definition["databases"]
                    .as_array()
                    .unwrap()
                    .contains(&serde_json::json!(database))
            );
        }
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
        let database = sink["database"].as_str().unwrap();
        let carrier_suite = format!("{database}.representation_carriers");
        assert!(
            sink["additional_suites"]
                .as_array()
                .unwrap()
                .iter()
                .any(|candidate| candidate == &serde_json::json!(carrier_suite)),
            "all LogicalValue variants and source representations must qualify {database}"
        );
        let carrier_definition = suites
            .iter()
            .find(|candidate| candidate["id"] == carrier_suite)
            .expect("every Sink must register a carrier qualification suite");
        assert_eq!(carrier_definition["required_for_live_qualified"], true);
        assert!(
            suite.get("source_fixtures").is_none(),
            "primary sink suites derive canonical fixtures instead of duplicating the roster"
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
            let native_type_suite =
                format!("{}.native_type_apply", sink["database"].as_str().unwrap());
            assert!(
                sink["additional_suites"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|entry| { entry == &serde_json::json!(native_type_suite) })
            );
            let native_type_definition = suites
                .iter()
                .find(|entry| entry["id"] == native_type_suite)
                .expect("every PostgreSQL Sink must qualify native logical values");
            assert_eq!(native_type_definition["required_for_live_qualified"], true);
            assert_eq!(native_type_definition["category"], "sink");
            assert_eq!(
                native_type_definition["source_fixtures"],
                serde_json::json!(["postgresql_15"])
            );
            assert!(
                native_type_definition["args"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|arg| {
                        arg.as_str().is_some_and(|arg| {
                            arg.contains(&format!("postgres{major}_writes_native_type_values"))
                        })
                    })
            );
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
