// Build-time sync of the vendored proto snippets used by
// `obs_build::config` for EventsConfig parsing.
//
// obs-proto owns the canonical `obs/v1/{options,enums}.proto`. obs-build
// needs those same bytes available at `include_str!` time, but
// `cargo publish` verifies each crate in isolation — a
// `../../obs-proto/proto/...` path breaks the dry-run + any consumer
// that pulls obs-build from crates.io. We keep a vendored copy under
// `crates/obs-build/proto/obs/v1/` and this build script fails the
// build when the two copies diverge in-repo.
//
// In a published tarball the canonical sibling doesn't exist, so the
// build script becomes a no-op and obs-build still compiles.

#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    clippy::expect_used,
    clippy::unwrap_used,
    missing_docs
)]

use std::path::{Path, PathBuf};

fn main() {
    let manifest_dir =
        PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR must be set"));
    let vendor_dir = manifest_dir.join("proto").join("obs").join("v1");
    let canonical_dir = manifest_dir
        .parent()
        .map(|p| p.join("obs-proto").join("proto").join("obs").join("v1"));

    for file in ["options.proto", "enums.proto"] {
        let vendored = vendor_dir.join(file);
        println!("cargo:rerun-if-changed={}", vendored.display());
        if let Some(ref dir) = canonical_dir {
            let canonical = dir.join(file);
            if canonical.exists() {
                println!("cargo:rerun-if-changed={}", canonical.display());
                check_in_sync(&canonical, &vendored);
            }
        }
        assert!(
            vendored.exists(),
            "missing vendored proto {}; obs-build cannot ship without it",
            vendored.display()
        );
    }
}

fn check_in_sync(canonical: &Path, vendored: &Path) {
    let Ok(canon_bytes) = std::fs::read(canonical) else {
        return;
    };
    let Ok(vendor_bytes) = std::fs::read(vendored) else {
        // Vendored copy will be flagged as missing by the caller's
        // assertion below.
        return;
    };
    assert!(
        canon_bytes == vendor_bytes,
        "vendored {} diverged from canonical {}. Re-run `cp {} {}` to resync, or update both \
         copies.",
        vendored.display(),
        canonical.display(),
        canonical.display(),
        vendored.display()
    );
}
