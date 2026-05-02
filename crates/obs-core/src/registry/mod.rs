//! Schema registry — `EventSchemaErased` object-safe trait, the
//! `linkme`-collected `EVENT_SCHEMAS` distributed slice, the runtime
//! `SchemaRegistry`, and the `ScrubbedEnvelope` worker→sink handoff.
//!
//! Spec 14.

mod erased;
mod scrubbed;

use std::{collections::HashMap, sync::Arc};

use linkme::distributed_slice;
use obs_proto::obs::v1::ObsEnvelope;

pub use self::{
    erased::{
        ArrowStructBuilder, DecodeError, EventSchemaErased, OtelAttributeView, OtlpValue,
        ScrubError, Sealed,
    },
    scrubbed::ScrubbedEnvelope,
};

/// The link-time distributed slice every `EventSchema` codegen
/// registers into. Walked once at observer init to build the runtime
/// `SchemaRegistry`. See spec 14 § 3.
///
/// **Cross-crate registration footgun**: cargo will not link an rlib
/// the binary doesn't reference. A schema-only crate must be
/// referenced from the binary (`use the_crate as _;`); see
/// `docs/research/spike-linkme.md`.
#[distributed_slice]
pub static EVENT_SCHEMAS: [&'static dyn EventSchemaErased] = [..];

/// Runtime registry: by-name and by-hash lookup populated from the
/// `linkme` distributed slice at observer init. Owned by
/// `StandardObserver`; sinks receive `Arc<SchemaRegistry>` at
/// construction.
#[derive(Clone)]
pub struct SchemaRegistry {
    by_name: HashMap<&'static str, &'static dyn EventSchemaErased>,
    by_hash: HashMap<u64, &'static dyn EventSchemaErased>,
}

impl std::fmt::Debug for SchemaRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut names: Vec<_> = self.by_name.keys().copied().collect();
        names.sort_unstable();
        f.debug_struct("SchemaRegistry")
            .field("len", &self.by_name.len())
            .field("names", &names)
            .finish()
    }
}

impl SchemaRegistry {
    /// Walk `EVENT_SCHEMAS` and assemble the runtime registry. Called
    /// once at `StandardObserver::build()`. Spec 14 § 4.
    #[must_use]
    pub fn from_link_section() -> Self {
        let mut by_name = HashMap::with_capacity(EVENT_SCHEMAS.len());
        let mut by_hash = HashMap::with_capacity(EVENT_SCHEMAS.len());
        for &schema in EVENT_SCHEMAS {
            by_name.insert(schema.full_name(), schema);
            by_hash.insert(schema.schema_hash(), schema);
        }
        Self { by_name, by_hash }
    }

    /// Empty registry. Useful for tests that don't care about decoding.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            by_name: HashMap::new(),
            by_hash: HashMap::new(),
        }
    }

    /// Hot-path lookup: try `schema_hash` first (8-byte u64), then
    /// fall back to `full_name` for foreign-producer interop.
    /// Spec 14 § 4.1.
    #[must_use]
    pub fn lookup(&self, env: &ObsEnvelope) -> Option<&'static dyn EventSchemaErased> {
        self.by_hash
            .get(&env.schema_hash)
            .copied()
            .or_else(|| self.by_name.get(env.full_name.as_str()).copied())
    }

    /// Number of registered schemas.
    #[must_use]
    pub fn len(&self) -> usize {
        self.by_name.len()
    }

    /// True if no schemas are registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_name.is_empty()
    }

    /// Iterate all registered schemas (used by `obs schema show`,
    /// `obs migrate`, the bridge pre-warm path).
    pub fn iter(&self) -> impl Iterator<Item = &'static dyn EventSchemaErased> + '_ {
        self.by_name.values().copied()
    }
}

impl Default for SchemaRegistry {
    fn default() -> Self {
        Self::from_link_section()
    }
}

/// Convenience: shared `Arc<SchemaRegistry>` for sink construction.
pub type SharedRegistry = Arc<SchemaRegistry>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_build_empty_when_no_schemas_registered() {
        let r = SchemaRegistry::empty();
        assert!(r.is_empty());
    }

    #[test]
    fn test_should_walk_link_section() {
        // The link section may be empty in tests until the test binary
        // pulls in obs-proto's built-ins via `use obs_proto as _;`.
        // We just assert the call returns without panicking.
        let _ = SchemaRegistry::from_link_section();
    }
}
