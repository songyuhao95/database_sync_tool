//! MySQL 5.7's version-owned structured Sink capability boundary.

use change_event::{
    CompatibilityError, CompatibilityInput, CompatibilityResult, ServerBuildIdentity,
    TargetCapabilityManifest,
};

pub fn compatibility_manifest(target_build: ServerBuildIdentity) -> TargetCapabilityManifest {
    mysql_source_contract::capability_manifest("5.7", "mysql57", target_build)
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
            "5.7.44",
            "mysql-5.7.44",
        ));
        assert!(manifest.verify_digest());
        manifest.validate().unwrap();
        assert!(manifest.capabilities.iter().any(|entry| {
            entry.source_logical_type == change_event::LogicalType::Boolean
                && entry.target.native_type == "tinyint(1)"
        }));
        assert!(manifest.capabilities.iter().any(|entry| {
            entry.source_logical_type == change_event::LogicalType::Uuid
                && entry.target.native_type == "char(36)"
        }));
    }
}
