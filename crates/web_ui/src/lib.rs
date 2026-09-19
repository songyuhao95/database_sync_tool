//! Authenticated Web API and SQLite-backed control data for the CDC UI.
//!
//! Database capture and apply behavior remains in the version-specific crates.
mod api;
mod auth;
mod auto_start;
mod catalog;
mod error;
mod frontend;
mod instances;
mod model;
mod registry;
mod runtime;
mod runtime_store;
mod secrets;
mod store;
mod task_worker;
mod tasks;

pub use api::{WebConfig, router};
pub use error::{Error, Result};
pub use store::Store;

#[cfg(test)]
mod tests;

mod source_logs;
