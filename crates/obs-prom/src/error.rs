//! Error type surfaced by [`PromRegistry::render`](crate::PromRegistry::render).

use std::io;

use thiserror::Error;

/// Errors returned by the render path. The underlying writer is the
/// only realistic failure mode; series-cap enforcement and idle
/// eviction are always infallible.
#[derive(Debug, Error)]
pub enum PromError {
    /// IO error on the supplied writer.
    #[error("render io: {0}")]
    Io(#[from] io::Error),
}
