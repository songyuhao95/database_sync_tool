use change_event::LogicalType;
use postgresql_16::{
    SourceExtension, SourceTypeCatalog, SourceTypeDefinition, SourceTypeField, source_type_mapping,
    source_type_mapping_with_catalog,
};

#[test]
fn versioned_mapping_keeps_postgresql_16_identity() {
    let mapping = source_type_mapping("timestamp(3) with time zone").unwrap();
    assert_eq!(mapping.connector.kind, "postgresql");
    assert_eq!(mapping.connector.version, "16");
    assert_eq!(
        mapping.logical_type,
        LogicalType::Instant {
            fractional_precision: 3,
        }
    );
}

#[test]
fn catalog_mapping_is_recursive_and_extension_qualified() {
    let catalog = SourceTypeCatalog::with_extensions(
        [
            SourceTypeDefinition::builtin(23, "pg_catalog", "integer"),
            SourceTypeDefinition::enum_type(8_000, "public", "state", ["new", "done"]),
            SourceTypeDefinition::domain(
                8_001,
                "public",
                "positive_id",
                23,
                ["VALUE > 0"],
                true,
                None,
            ),
            SourceTypeDefinition::composite(
                8_002,
                "public",
                "item",
                [
                    SourceTypeField::new("id", 8_001, false),
                    SourceTypeField::new("state", 8_000, true),
                ],
            ),
        ],
        [SourceExtension {
            name: "postgis".into(),
            version: "3.4.0".into(),
            schema: "public".into(),
            installed: true,
            available: true,
            target_compatible: None,
        }],
    );

    assert!(matches!(
        source_type_mapping_with_catalog("public.item", &catalog)
            .unwrap()
            .logical_type,
        LogicalType::Struct { ref fields }
            if fields.iter().map(|field| field.name.as_str()).collect::<Vec<_>>()
                == ["id", "state"]
    ));
}
