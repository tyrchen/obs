//! Public configuration enums shared between the builder and the writer.

use serde::{Deserialize, Serialize};

/// Layout choice for the analytical store. Spec 22 § 1.2.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[non_exhaustive]
pub enum ParquetLayout {
    /// One sparse table; all events written to `obs_events.parquet`
    /// with per-event-type struct columns.
    #[default]
    Single,
    /// One file per event type. Opt-in for very-high-volume splits.
    TablePerEvent,
}

/// Parquet compression codec. Mirrors a subset of
/// `parquet::basic::Compression`. Spec 22 § 2.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[non_exhaustive]
pub enum ParquetCompression {
    /// Snappy — moderate compression, fast (default).
    #[default]
    Snappy,
    /// Zstd — best compression, slowest.
    Zstd,
    /// LZ4 raw — fast, lower ratio.
    Lz4,
    /// No compression.
    Uncompressed,
}

impl ParquetCompression {
    /// Convert to the parquet crate's enum.
    #[must_use]
    pub fn to_parquet(self) -> parquet::basic::Compression {
        match self {
            Self::Snappy => parquet::basic::Compression::SNAPPY,
            Self::Lz4 => parquet::basic::Compression::LZ4,
            Self::Uncompressed => parquet::basic::Compression::UNCOMPRESSED,
            Self::Zstd => parquet::basic::Compression::ZSTD(
                parquet::basic::ZstdLevel::try_new(3).unwrap_or_default(),
            ),
        }
    }
}
