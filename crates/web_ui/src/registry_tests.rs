use super::super::registry::{AdapterKind, ConnectorIdentity, SinkRegistry, SourceRegistry};

#[test]
fn source_and_sink_registries_publish_independent_connector_catalogs() {
    let sources = SourceRegistry;
    let sinks = SinkRegistry;

    let source_ids = sources
        .all()
        .map(|connector| connector.identity)
        .collect::<Vec<_>>();
    let sink_ids = sinks
        .all()
        .map(|connector| connector.identity)
        .collect::<Vec<_>>();

    assert_eq!(
        source_ids,
        vec![
            ConnectorIdentity::new("mysql", "5.7"),
            ConnectorIdentity::new("mysql", "8.0"),
            ConnectorIdentity::new("mysql", "8.4"),
            ConnectorIdentity::new("postgresql", "15"),
        ]
    );
    assert_eq!(sink_ids, source_ids);
    assert!(sources.find("postgresql", "15").is_some());
    assert!(sinks.find("postgresql", "15").is_some());
    assert_eq!(
        sinks.find("postgresql", "15").unwrap().adapter,
        AdapterKind::Postgresql15
    );
}

#[test]
fn registry_lookup_rejects_unsupported_connector_without_nearest_version_fallback() {
    assert!(SourceRegistry.find("mysql", "8.1").is_none());
    assert!(SinkRegistry.find("postgresql", "14").is_none());
    assert!(SourceRegistry.find("postgresql", "8.4").is_none());
}

#[test]
fn source_and_sink_capabilities_are_role_specific() {
    let source = SourceRegistry
        .find("postgresql", "15")
        .expect("PostgreSQL 15 Source is registered");
    let sink = SinkRegistry
        .find("postgresql", "15")
        .expect("PostgreSQL 15 Sink is registered");

    assert_eq!(source.role, "source");
    assert_eq!(source.adapter, AdapterKind::Postgresql15);
    assert!(source.capabilities.supports_transactions);
    assert!(source.capabilities.supports_primary_key_rows);
    assert_eq!(sink.role, "sink");
    assert!(sink.capabilities.supports_transactions);
    assert!(sink.capabilities.supports_primary_key_rows);
    assert_ne!(source.capabilities, sink.capabilities);
}

#[test]
fn source_and_sink_registries_have_no_pair_specific_registration() {
    let sources = SourceRegistry.all().collect::<Vec<_>>();
    let sinks = SinkRegistry.all().collect::<Vec<_>>();
    assert_eq!(sources.len() * sinks.len(), 16);
    for source in sources {
        assert_eq!(source.role, "source");
        assert!(
            SourceRegistry
                .find(source.identity.kind, source.identity.version)
                .is_some()
        );
        for sink in &sinks {
            assert_eq!(sink.role, "sink");
            assert!(
                SinkRegistry
                    .find(sink.identity.kind, sink.identity.version)
                    .is_some()
            );
        }
    }
}
