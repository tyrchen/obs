// Keep the vendored `proto/obs/v1/{options,enums}.proto` copies in
// sync with the canonical ones in `obs-proto/proto/obs/v1/`.
// See `crates/obs-build/build.rs` for the same pattern + rationale.

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
    let canonical_dir = manifest_dir.parent().and_then(|p| p.parent()).map(|p| {
        p.join("crates")
            .join("obs-proto")
            .join("proto")
            .join("obs")
            .join("v1")
    });

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
            "missing vendored proto {}; obs-cli cannot ship without it",
            vendored.display()
        );
    }
}

fn check_in_sync(canonical: &Path, vendored: &Path) {
    let Ok(canon_bytes) = std::fs::read(canonical) else {
        return;
    };
    let Ok(vendor_bytes) = std::fs::read(vendored) else {
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
