use change_event::{LogicalType, ServerBuildIdentity, SinkAdapter as _};

#[test]
fn postgres17_sink_has_its_own_versioned_contract() {
    let sink = postgresql_17::SinkAdapter::new();
    let manifest = sink.capability_manifest();
    assert_eq!(manifest.connector, "postgresql_17");
    assert_eq!(manifest.target, "postgresql-17");
    assert!(manifest.supported_logical_types.contains(&"multirange"));
    assert!(manifest.supported_logical_types.contains(&"custom"));
}

#[test]
fn postgres17_structured_manifest_is_versioned_and_digest_valid() {
    let manifest = postgresql_17::target_capability_manifest(ServerBuildIdentity::new(
        "postgresql",
        "community",
        "17.2",
        "postgres-17.2",
    ));
    assert_eq!(manifest.connector.version, "17");
    assert!(manifest.verify_digest());
    manifest.validate().unwrap();
    assert!(manifest.capabilities.iter().any(|entry| {
        matches!(entry.source_logical_type, LogicalType::MultiRange { .. })
            && entry.rule.id.contains("postgresql17")
    }));
}
