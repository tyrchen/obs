#!/usr/bin/env bash
# Strict-superset clippy run for `make lint-strict`. Spec 90 § M4 /
# impl-plan task 5.3.
#
# Translates `scripts/pedantic-allows.txt` into a `-A clippy::<lint>`
# flag list, then invokes:
#
#   cargo clippy --workspace --all-targets --all-features -- \
#     -D warnings -W clippy::pedantic <allows>
#
# Lines starting with `#` and inline `# ...` comments in the allow
# list are ignored. Unknown lint names are downgraded to warnings via
# `#[allow(unknown_lints)]` semantics implicitly: cargo aborts on
# an unknown lint name at the command line, so the `# inline` comment
# stripping must produce a clean list.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ALLOW_FILE="$ROOT/scripts/pedantic-allows.txt"

if [[ ! -f "$ALLOW_FILE" ]]; then
  echo "lint-strict: $ALLOW_FILE not found" >&2
  exit 1
fi

# Strip blank lines and `#` comments (both whole-line and inline).
# bash 3 (macOS) lacks `mapfile`, so go through a `while read` loop.
ARGS=(-D warnings -W clippy::pedantic)
while IFS= read -r lint; do
  [[ -n "$lint" ]] || continue
  ARGS+=(-A "clippy::${lint}")
done < <(
  sed -E 's/#.*$//; s/[[:space:]]+$//' "$ALLOW_FILE" \
    | awk 'NF { print $1 }'
)

# Also tolerate unknown lint names — cargo would abort otherwise on a
# clippy-renamed-or-removed lint, which is a non-actionable break for
# us. The deny stays on actual warnings.
ARGS+=(-A unknown_lints -A renamed_and_removed_lints)

exec cargo clippy --workspace --all-targets --all-features -- "${ARGS[@]}"
