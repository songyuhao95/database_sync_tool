use change_event::ServerBuildIdentity;
use postgresql_15::{capability_manifest_for_version, compatibility_manifest_for_version};

#[test]
fn unknown_postgresql_versions_have_no_synthetic_sink_capabilities() {
    let capability_manifest = capability_manifest_for_version("18");
    assert!(capability_manifest.supported_logical_types.is_empty());
    assert!(capability_manifest.supported_presence.is_empty());

    let target_build =
        ServerBuildIdentity::new("postgresql", "community", "18.0", "postgres-180000");
    let structured = compatibility_manifest_for_version(target_build, "18");
    assert_eq!(structured.connector.version, "18");
    assert!(structured.capabilities.is_empty());
}

#[test]
fn supported_postgresql_versions_keep_their_qualified_capabilities() {
    for version in ["15", "16", "17"] {
        assert!(
            !capability_manifest_for_version(version)
                .supported_logical_types
                .is_empty()
        );
        let target_build = ServerBuildIdentity::new(
            "postgresql",
            "community",
            format!("{version}.0"),
            format!("postgres-{version}"),
        );
        assert!(
            !compatibility_manifest_for_version(target_build, version)
                .capabilities
                .is_empty()
        );
    }
}
