use change_event::{LogicalType, SourceAdapter};

#[test]
fn versioned_mapping_and_source_adapter_are_exposed() {
    let mapping = postgresql_17::source_type_mapping("inet").unwrap();
    assert_eq!(mapping.connector.kind, "postgresql");
    assert_eq!(mapping.connector.version, "17");
    assert_eq!(mapping.mapping_version, postgresql_17::MAPPING_VERSION);
    assert!(matches!(mapping.logical_type, LogicalType::Network { .. }));

    fn assert_source_adapter<T: SourceAdapter>() {}
    assert_source_adapter::<postgresql_17::Replication>();
}
