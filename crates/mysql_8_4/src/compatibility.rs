//! MySQL 8.4's version-owned structured Sink capability boundary.

use change_event::{
    CompatibilityError, CompatibilityInput, CompatibilityResult, ConnectorIdentity,
    ServerBuildIdentity, SourceTypeMapping, TargetCapabilityManifest,
};

const CONNECTOR_KIND: &str = "mysql";
const CONNECTOR_VERSION: &str = "8.4";

/// MySQL 8.4 publishes an independent source identity while reusing the
/// version-neutral MySQL declaration contract.
pub fn source_type_mapping(
    native_type: &str,
    charset: Option<&str>,
    collation: Option<&str>,
) -> Result<SourceTypeMapping, String> {
    let mut mapping = mysql_5_7::source_type_mapping(native_type, charset, collation)
        .map_err(|error| error.to_string())?;
    mapping.connector = ConnectorIdentity::new(CONNECTOR_KIND, CONNECTOR_VERSION);
    mapping.mapping_id = mapping.mapping_id.strip_prefix("mysql57.").map_or_else(
        || "mysql84.source-type.unknown".to_owned(),
        |id| format!("mysql84.{id}"),
    );
    mapping.mapping_version = "mysql-8.4.source-type-mapping.v1".to_owned();
    let bytes = serde_json::to_vec(&mapping).expect("MySQL mapping evidence is serializable");
    use sha2::{Digest as _, Sha256};
    let digest = Sha256::digest(bytes);
    mapping.evidence_digest = Some(digest.iter().map(|byte| format!("{byte:02x}")).collect());
    Ok(mapping)
}

pub fn compatibility_manifest(target_build: ServerBuildIdentity) -> TargetCapabilityManifest {
    mysql_source_contract::capability_manifest("8.4", "mysql84", target_build)
}

pub fn target_capability_manifest(target_build: ServerBuildIdentity) -> TargetCapabilityManifest {
    compatibility_manifest(target_build)
}

pub fn structured_capability_manifest(
    target_build: ServerBuildIdentity,
) -> TargetCapabilityManifest {
    compatibility_manifest(target_build)
}

pub fn capability_manifest_for(target_build: ServerBuildIdentity) -> TargetCapabilityManifest {
    compatibility_manifest(target_build)
}

pub fn plan_compatibility(
    input: CompatibilityInput<'_>,
) -> Result<CompatibilityResult, CompatibilityError> {
    change_event::plan_compatibility(input)
}

impl crate::sql::SinkAdapter {
    pub fn structured_capability_manifest(
        &self,
        target_build: ServerBuildIdentity,
    ) -> TargetCapabilityManifest {
        compatibility_manifest(target_build)
    }

    pub fn plan_compatibility(
        &self,
        input: CompatibilityInput<'_>,
    ) -> Result<CompatibilityResult, CompatibilityError> {
        plan_compatibility(input)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_is_versioned_and_contains_cross_connector_conversions() {
        let manifest = compatibility_manifest(ServerBuildIdentity::new(
            "mysql",
            "oracle",
            "8.4.8",
            "mysql-8.4.8",
        ));
        assert!(manifest.verify_digest());
        manifest.validate().unwrap();
        assert!(manifest.capabilities.iter().any(|entry| {
            entry.source_logical_type == change_event::LogicalType::Boolean
                && entry.target.native_type == "tinyint(1)"
        }));
    }
}
