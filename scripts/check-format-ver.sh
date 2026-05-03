#!/usr/bin/env bash
# Fails if `crates/obs-proto/proto/obs/v1/envelope.proto` was modified
# in this diff range without a corresponding change to
# `ENVELOPE_FORMAT_VER` in `crates/obs-proto/src/lib.rs`.
#
# Spec 90 § 3.3 / impl-plan task 5.5 — the envelope wire shape is
# locked at v1.0; any change forces a bump and a major release.
#
# Usage:
#   scripts/check-format-ver.sh <base-ref> <head-ref>
#
# In CI we pass `origin/main HEAD`; locally `git merge-base ... HEAD`
# also works.

set -euo pipefail

# Resolve the base ref. Prefer the explicit positional or env var; if
# neither was set, probe `origin/main` then `origin/master` then
# `main`/`master` so the same script works locally and in CI without
# the caller having to know the project's default branch.
BASE="${1:-${BASE_REF:-}}"
HEAD="${2:-${HEAD_REF:-HEAD}}"
if [[ -z "$BASE" ]]; then
  for candidate in origin/main origin/master main master; do
    if git rev-parse --verify "$candidate" >/dev/null 2>&1; then
      BASE="$candidate"
      break
    fi
  done
fi
if [[ -z "$BASE" ]]; then
  echo "check-format-ver: no base ref found; pass one as the first arg" >&2
  exit 1
fi

ENVELOPE_PROTO="crates/obs-proto/proto/obs/v1/envelope.proto"
LIB_RS="crates/obs-proto/src/lib.rs"
SENTINEL_NAME="ENVELOPE_FORMAT_VER"

# Diff name list. Empty diff exits silently with code 0.
changed=$(git diff --name-only "$BASE" "$HEAD" -- "$ENVELOPE_PROTO" "$LIB_RS")

if ! grep -Fq "$ENVELOPE_PROTO" <<<"$changed"; then
  exit 0
fi

# envelope.proto was touched — require a co-edit of the sentinel.
if ! grep -Fq "$LIB_RS" <<<"$changed"; then
  echo "::error::$ENVELOPE_PROTO changed without bumping $SENTINEL_NAME in $LIB_RS." >&2
  echo "Spec 90 § 3.3: envelope wire-shape changes require a format_ver bump." >&2
  exit 1
fi

# Verify the sentinel const itself moved in the patch (not just an
# unrelated edit elsewhere in lib.rs).
if ! git diff "$BASE" "$HEAD" -- "$LIB_RS" | grep -Eq "^[-+].*${SENTINEL_NAME}"; then
  echo "::error::$ENVELOPE_PROTO changed; $LIB_RS was edited but $SENTINEL_NAME line is unchanged." >&2
  echo "Spec 90 § 3.3: bump $SENTINEL_NAME alongside the wire-shape change." >&2
  exit 1
fi

echo "format-ver guard: envelope.proto change pairs with $SENTINEL_NAME bump — ok." >&2
