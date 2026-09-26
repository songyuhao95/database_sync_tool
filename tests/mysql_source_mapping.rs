use change_event::ServerBuildIdentity;

#[test]
fn mysql_releases_publish_independent_source_mapping_identity() {
    let mappings = [
        mysql_5_7::source_type_mapping("point srid 4326", None, None).unwrap(),
        mysql_8_0::source_type_mapping("point srid 4326", None, None).unwrap(),
        mysql_8_4::source_type_mapping("point srid 4326", None, None).unwrap(),
    ];

    assert_eq!(mappings[0].connector.version, "5.7");
    assert_eq!(mappings[1].connector.version, "8.0");
    assert_eq!(mappings[2].connector.version, "8.4");
    assert_eq!(mappings[0].logical_type, mappings[1].logical_type);
    assert_eq!(mappings[1].logical_type, mappings[2].logical_type);
    assert_eq!(
        mappings[0].mapping_version,
        "mysql-5.7.source-type-mapping.v1"
    );
    assert_eq!(
        mappings[1].mapping_version,
        "mysql-8.0.source-type-mapping.v1"
    );
    assert_eq!(
        mappings[2].mapping_version,
        "mysql-8.4.source-type-mapping.v1"
    );
    assert_ne!(mappings[0].evidence_digest, mappings[1].evidence_digest);
    assert_ne!(mappings[1].evidence_digest, mappings[2].evidence_digest);
}

#[test]
fn mapping_evidence_digest_is_bound_to_definition_build_and_environment() {
    let mapping = mysql_5_7::source_type_mapping("decimal(30,6)", None, None).unwrap();
    let build = ServerBuildIdentity::new("mysql", "oracle", "5.7.44", "mysql-5.7.44");
    let qualified = mapping.clone().with_source_evidence(
        "schema-fingerprint-a",
        build.clone(),
        "environment-a",
    );
    let changed_definition = mapping.clone().with_source_evidence(
        "schema-fingerprint-b",
        build.clone(),
        "environment-a",
    );
    let changed_build = mapping.with_source_evidence(
        "schema-fingerprint-a",
        ServerBuildIdentity::new("mysql", "oracle", "5.7.45", "mysql-5.7.45"),
        "environment-a",
    );

    assert_eq!(
        qualified.source_definition_fingerprint.as_deref(),
        Some("schema-fingerprint-a")
    );
    assert_eq!(qualified.source_build, Some(build));
    assert_eq!(
        qualified.environment_fingerprint.as_deref(),
        Some("environment-a")
    );
    assert!(qualified.evidence_digest.is_some());
    assert_ne!(
        qualified.evidence_digest,
        changed_definition.evidence_digest
    );
    assert_ne!(qualified.evidence_digest, changed_build.evidence_digest);
}

#[test]
fn versioned_numeric_declarations_keep_mysql_float_storage_width_semantics() {
    use change_event::LogicalType;
    use change_event::SourceTypeMapping;

    fn map_57(
        native: &str,
        charset: Option<&str>,
        collation: Option<&str>,
    ) -> Result<SourceTypeMapping, String> {
        mysql_5_7::source_type_mapping(native, charset, collation)
            .map_err(|error| error.to_string())
    }

    for (map, expected_float_24) in [
        (
            map_57 as fn(&str, Option<&str>, Option<&str>) -> Result<_, _>,
            64,
        ),
        (mysql_8_0::source_type_mapping, 32),
        (mysql_8_4::source_type_mapping, 64),
    ] {
        assert_eq!(
            map("float(24)", None, None).unwrap().logical_type,
            LogicalType::float(expected_float_24),
            "FLOAT(24) storage width differs by MySQL server release"
        );
        assert_eq!(
            map("float(25)", None, None).unwrap().logical_type,
            LogicalType::float(64)
        );
        assert_eq!(
            map("float(7,4)", None, None).unwrap().logical_type,
            LogicalType::float(32)
        );
        assert_eq!(
            map("decimal unsigned zerofill", None, None)
                .unwrap()
                .logical_type,
            LogicalType::decimal(10, 0)
        );
    }
}

#[test]
fn every_rostered_mysql_declaration_example_has_a_versioned_source_mapping() {
    use change_event::SourceTypeMapping;
    use serde_json::Value;

    type Mapping = fn(&str, Option<&str>, Option<&str>) -> Result<SourceTypeMapping, String>;
    fn map_57(
        native: &str,
        charset: Option<&str>,
        collation: Option<&str>,
    ) -> Result<SourceTypeMapping, String> {
        mysql_5_7::source_type_mapping(native, charset, collation)
            .map_err(|error| error.to_string())
    }
    let inventory: Value =
        serde_json::from_str(include_str!("../scripts/type-inventory.json")).unwrap();
    let connectors = [
        ("mysql_5_7", map_57 as Mapping),
        ("mysql_8_0", mysql_8_0::source_type_mapping as Mapping),
        ("mysql_8_4", mysql_8_4::source_type_mapping as Mapping),
    ];

    for (connector, map) in connectors {
        let profiles = inventory["native_declaration_profiles"]
            .as_object()
            .unwrap();
        let mut declarations = 0;
        for (profile_id, profile) in profiles {
            if !profile["connectors"]
                .as_array()
                .unwrap()
                .iter()
                .any(|id| id.as_str() == Some(connector))
            {
                continue;
            }
            for native_type in profile["examples"].as_array().unwrap() {
                let native_type = native_type.as_str().unwrap();
                let mapping = map(native_type, Some("utf8mb4"), Some("utf8mb4_bin"))
                    .unwrap_or_else(|error| {
                        panic!("{connector} roster profile {profile_id}, {native_type}: {error}")
                    });
                assert_eq!(mapping.connector.kind, "mysql");
                declarations += 1;
            }
            if let Some(aliases) = inventory["mapping_aliases"][profile_id].as_array() {
                for alias in aliases.iter().filter_map(Value::as_str).filter(|alias| {
                    !alias.is_empty()
                        && alias
                            .bytes()
                            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
                }) {
                    let mapping =
                        map(alias, Some("utf8mb4"), Some("utf8mb4_bin")).unwrap_or_else(|error| {
                            panic!("{connector} alias profile {profile_id}, {alias}: {error}")
                        });
                    assert_eq!(mapping.connector.kind, "mysql");
                    declarations += 1;
                }
            }
        }
        assert!(declarations >= 40, "{connector} roster coverage regressed");
    }
}
