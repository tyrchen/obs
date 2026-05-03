//! Shared file-tree scanners reused by `obs lint` and `obs audit`. Spec
//! 93 P3-6.

use std::path::Path;

/// Count occurrences of `forensic!(` (or `obs::forensic!(`) across all
/// `.rs` files under `root`. Used as a conservative proxy for the
/// number of forensic callsites in a project.
///
/// Returns `None` if `root` cannot be read; `Some(count)` otherwise.
#[must_use]
pub fn scan_forensic_count(root: &Path) -> Option<usize> {
    let mut count = 0usize;
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = std::fs::read_dir(&dir).ok()?;
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_dir() {
                stack.push(p);
                continue;
            }
            if p.extension().and_then(|e| e.to_str()) != Some("rs") {
                continue;
            }
            let Ok(content) = std::fs::read_to_string(&p) else {
                continue;
            };
            count += content.matches("forensic!(").count();
        }
    }
    Some(count)
}
