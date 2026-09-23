use super::super::{catalog::CatalogColumn, registry::SourceRegistry};
use change_event::ServerBuildIdentity;

#[test]
fn postgresql_source_mapping_keeps_catalog_and_server_evidence() {
    let source = SourceRegistry
        .find("postgresql", "16")
        .expect("PostgreSQL 16 source is registered");
    let catalog =
        postgresql_15::SourceTypeCatalog::new([postgresql_15::SourceTypeDefinition::enum_type(
            9_001,
            "public",
            "state",
            ["new", "done"],
        )]);
    let build = ServerBuildIdentity::new(
        "postgresql",
        "community",
        "16.10",
        "PostgreSQL 16.10 on x86_64-pc-windows-msvc",
    );
    let mapping = source
        .source_type_mapping_with_evidence(
            &CatalogColumn {
                name: "state".into(),
                column_type: "public.state".into(),
                nullable: false,
                extra: String::new(),
                collation: None,
                default_value: None,
            },
            Some(&catalog),
            Some(build.clone()),
            Some("pg-environment-fingerprint"),
        )
        .expect("catalog-backed source mapping should qualify");

    assert_eq!(mapping.connector.version, "16");
    assert_eq!(mapping.source_build, Some(build));
    assert_eq!(
        mapping.environment_fingerprint.as_deref(),
        Some("pg-environment-fingerprint")
    );
    assert!(mapping.source_definition_fingerprint.is_some());
}
