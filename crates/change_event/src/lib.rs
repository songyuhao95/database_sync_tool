//! Database-independent ChangeEvent v0.3 model and validation. No database driver dependency.
use serde::{Deserialize, Serialize};

mod compatibility;
mod json;
mod model;
mod reader;
mod snapshot;
mod validate;
pub use snapshot::*;

pub use compatibility::*;

pub use json::json;
pub use model::*;
pub use reader::JsonReader;
pub use validate::{
    ChangeEventValidationError, SourceContractError, TargetCapabilityFailure, ValidatedTransaction,
    ValidationError, validate, validate_value,
};

/// The outcome of checking the destination's authoritative replication
/// metadata after a commit acknowledgement was lost.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CommitResolution {
    Applied,
    NotApplied,
    Unprovable,
}

/// Stable runtime categories used by Sink adapters and the Web worker.
///
/// The category is deliberately independent of a vendor driver's error type.
/// It is the retry boundary: only connection and lock failures may replay the
/// complete source transaction. A commit acknowledgement is never classified
/// as an ordinary retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TargetApplyErrorKind {
    Conversion,
    Capability,
    Constraint,
    Sql,
    Connection,
    LockTimeout,
    CommitUnknown,
    CheckpointConflict,
}

impl TargetApplyErrorKind {
    pub const fn is_retryable(self) -> bool {
        matches!(self, Self::Connection | Self::LockTimeout)
    }

    pub const fn retry_classification(self) -> RetryClassification {
        match self {
            Self::Connection | Self::LockTimeout => RetryClassification::Retryable,
            Self::CommitUnknown => RetryClassification::CommitUnknown,
            Self::Conversion
            | Self::Capability
            | Self::Constraint
            | Self::Sql
            | Self::CheckpointConflict => RetryClassification::NotRetryable,
        }
    }

    pub const fn failure_class(self) -> FailureClass {
        match self {
            Self::Connection | Self::LockTimeout => FailureClass::TransientTarget,
            Self::CommitUnknown => FailureClass::CommitUnknown,
            Self::Conversion | Self::Capability => FailureClass::TargetCapability,
            Self::Constraint | Self::Sql | Self::CheckpointConflict => FailureClass::InvalidInput,
        }
    }

    pub const fn phase(self) -> FailurePhase {
        match self {
            Self::Conversion => FailurePhase::Conversion,
            Self::Capability => FailurePhase::CapabilityQualification,
            Self::Constraint | Self::Sql | Self::CheckpointConflict => FailurePhase::Apply,
            Self::Connection | Self::LockTimeout => FailurePhase::Apply,
            Self::CommitUnknown => FailurePhase::Commit,
        }
    }

    pub const fn stable_code(self) -> &'static str {
        match self {
            Self::Conversion => "target_apply.conversion_failed",
            Self::Capability => "target_apply.capability_failed",
            Self::Constraint => "target_apply.constraint_failed",
            Self::Sql => "target_apply.sql_error",
            Self::Connection => "target_apply.connection_failed",
            Self::LockTimeout => "target_apply.lock_timeout",
            Self::CommitUnknown => "target_apply.commit_unknown",
            Self::CheckpointConflict => "target_apply.checkpoint_conflict",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct CapabilityManifest {
    pub connector: &'static str,
    pub target: &'static str,
    pub contract: &'static str,
    pub supported_logical_types: &'static [&'static str],
    pub supported_presence: &'static [&'static str],
    pub requires_primary_key: bool,
}

/// Database-neutral boundary implemented by every target connector.
pub trait SinkAdapter {
    type Plan;
    type Error;

    fn capability_manifest(&self) -> CapabilityManifest;
    fn qualify(&self, transaction: &ValidatedTransaction) -> std::result::Result<(), Self::Error>;
    fn plan(
        &self,
        transaction: &ValidatedTransaction,
    ) -> std::result::Result<Self::Plan, Self::Error>;
}

/// Structured compatibility boundary shared by Web preflight, activation, and
/// runtime callers. Existing SinkAdapter implementations receive the default
/// database-neutral planner without adding source-target pair dispatch.
pub trait SinkCompatibilityAdapter: SinkAdapter {
    fn explain_compatibility(
        &self,
        input: CompatibilityInput<'_>,
    ) -> std::result::Result<CompatibilityResult, CompatibilityError> {
        compatibility::explain_compatibility(input)
    }

    fn plan_compatibility(
        &self,
        input: CompatibilityInput<'_>,
    ) -> std::result::Result<CompatibilityResult, CompatibilityError> {
        compatibility::plan_compatibility(input)
    }
}

impl<T: SinkAdapter + ?Sized> SinkCompatibilityAdapter for T {}

/// Database-neutral boundary for a source connector that publishes complete, validated
/// transaction batches. The associated error remains connector-owned so the core crate does
/// not depend on a driver or an error taxonomy.
pub trait SourceAdapter {
    type Error;

    fn next_transaction(
        &mut self,
    ) -> std::result::Result<Option<ValidatedTransaction>, Self::Error>;
}

#[cfg(test)]
mod tests;
