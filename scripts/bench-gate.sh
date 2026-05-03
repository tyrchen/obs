#!/usr/bin/env bash
# Compare local criterion numbers against the checked-in baseline and
# fail when any named bench regresses by more than the configured
# threshold (default 10%).
#
# Spec 94 § 3.6 / D7-5. Wired into the nightly bench-gate workflow.

set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

THRESHOLD_PCT="${BENCH_GATE_THRESHOLD_PCT:-10}"

cargo bench --workspace --quiet

ok=0
fail=0

compare_one() {
  local crate="$1"
  local baseline="crates/${crate}/benches/baseline.json"
  if [[ ! -f "$baseline" ]]; then
    echo "no baseline checked in for ${crate}; skipping" >&2
    return 0
  fi
  python3 - "$crate" "$baseline" "$THRESHOLD_PCT" <<'PY' || fail=$((fail+1))
import json
import sys
from pathlib import Path

crate, baseline_path, threshold = sys.argv[1:]
threshold_pct = float(threshold)
baselines = {
    rec["name"]: rec["mean_ns"]
    for rec in json.loads(Path(baseline_path).read_text())["baselines"]
}
root = Path("target/criterion")
fail = False
for name, prev in sorted(baselines.items()):
    estimates = root / name / "new" / "estimates.json"
    if not estimates.exists():
        print(f"[bench-gate] {crate}::{name} missing in this run", file=sys.stderr)
        continue
    data = json.loads(estimates.read_text())
    cur = data.get("mean", {}).get("point_estimate")
    if cur is None:
        continue
    delta_pct = (cur - prev) / prev * 100.0 if prev else 0.0
    marker = "OK"
    if delta_pct > threshold_pct:
        marker = "REGRESSION"
        fail = True
    elif delta_pct < -threshold_pct:
        marker = "IMPROVEMENT"
    print(f"[bench-gate] {crate}::{name}: {prev:.0f} ns -> {cur:.0f} ns ({delta_pct:+.1f}%) {marker}")
sys.exit(1 if fail else 0)
PY
  if [[ $? -eq 0 ]]; then
    ok=$((ok+1))
  fi
}

for crate in obs-core obs-tracing-bridge; do
  compare_one "$crate"
done

if [[ $fail -gt 0 ]]; then
  echo "::error::bench-gate: ${fail} crate(s) regressed beyond ${THRESHOLD_PCT}%" >&2
  exit 1
fi
echo "bench-gate: ${ok} crate(s) within ${THRESHOLD_PCT}% threshold."
