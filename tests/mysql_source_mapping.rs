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
