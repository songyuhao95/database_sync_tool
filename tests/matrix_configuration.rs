use serde_json::Value;

#[test]
fn issue_15_declares_one_local_four_by_four_change_event_matrix() {
    let matrix: Value = serde_json::from_str(include_str!("../scripts/test-matrix.json"))
        .expect("test matrix must be valid JSON");
    let databases = matrix["databases"]
        .as_array()
        .expect("matrix databases must be an array");
    let expected = ["mysql_5_7", "mysql_8_0", "mysql_8_4", "postgresql_15"];
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
    assert_eq!(suite_databases, expected);
    let stages = suite["stages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|value| value.as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(stages, ["ChangeEvent", "Sql"]);
}

#[test]
fn issue_37_declares_component_qualification_instead_of_sixteen_live_routes() {
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
        vec!["mysql_5_7", "mysql_8_0", "mysql_8_4", "postgresql_15"]
    );

    let sources = config["live_qualification"]["sources"].as_array().unwrap();
    let sinks = config["live_qualification"]["sinks"].as_array().unwrap();
    assert_eq!(sources.len(), 4);
    assert_eq!(sinks.len(), 4);
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

    let suites = matrix["suites"].as_array().unwrap();
    let find = |id: &str| suites.iter().find(|suite| suite["id"] == id).unwrap();
    assert_eq!(
        suites
            .iter()
            .filter(|suite| suite["category"] == "source")
            .count(),
        4
    );
    assert_eq!(
        suites
            .iter()
            .filter(|suite| suite["category"] == "sink")
            .count(),
        4
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
    for sink in sinks {
        let suite = find(sink["suite"].as_str().unwrap());
        let fixtures = suite["source_fixtures"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value.as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            fixtures, roster,
            "every Sink must consume all four source fixtures"
        );
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
