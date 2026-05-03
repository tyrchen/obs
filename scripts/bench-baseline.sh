#!/usr/bin/env bash
# Refresh the checked-in criterion baseline snapshot.
#
# Spec 94 § 3.6 / D7-5: each crate's `benches/baseline.json` is the
# stable "this is acceptable" reference that `make bench-gate` compares
# against. Re-running this script after a perf-affecting change is the
# operator's signal to update the snapshot — commit the JSON alongside
# the change.

set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

# Honour `CARGO_TARGET_DIR` / `.cargo/config.toml`'s `target-dir` so
# the script works when the workspace's compiled artefacts live
# outside the repo (common when developers share a global cache).
TARGET_DIR="$(cargo metadata --no-deps --format-version 1 \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"

cargo bench --workspace --quiet

write_baseline() {
  local crate="$1"
  local out="crates/${crate}/benches/baseline.json"
  if [[ ! -d "${TARGET_DIR}/criterion" ]]; then
    echo "no criterion output for ${crate} at ${TARGET_DIR}/criterion; run 'cargo bench' first" >&2
    return 1
  fi
  python3 - "$crate" "$out" "$TARGET_DIR" <<'PY'
import json
import os
import sys
from pathlib import Path

crate = sys.argv[1]
out_path = Path(sys.argv[2])
target_dir = Path(sys.argv[3])
root = target_dir / "criterion"
records = []
if not root.is_dir():
    sys.exit(0)
for group in sorted(root.iterdir()):
    if not group.is_dir():
        continue
    estimates = group / "new" / "estimates.json"
    if not estimates.exists():
        continue
    data = json.loads(estimates.read_text())
    mean_ns = data.get("mean", {}).get("point_estimate")
    if mean_ns is None:
        continue
    records.append({"name": group.name, "mean_ns": mean_ns})
out_path.parent.mkdir(parents=True, exist_ok=True)
out_path.write_text(json.dumps({"baselines": records}, indent=2, sort_keys=True))
print(f"wrote {len(records)} baselines to {out_path}")
PY
}

for crate in obs-core obs-tracing-bridge; do
  write_baseline "$crate"
done

echo "bench baseline refreshed; commit any updated baseline.json files."
