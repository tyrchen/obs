#!/usr/bin/env bash
# Publish every not-yet-published obs crate in dependency order.
#
# Usage:
#   scripts/publish-all.sh              # publish anything crates.io doesn't already have
#   scripts/publish-all.sh --dry-run    # dry-run each crate, skipping already-published
#
# Handles two operational hazards:
#
# 1. crates.io's new-crate rate limit (HTTP 429 with a body of the form
#    `...try again after Fri, 08 May 2026 04:30:26 GMT...`). We parse
#    the timestamp out of the error body and sleep exactly that long
#    (plus a small jitter) so we come back at the first legal moment
#    instead of burning 60s/120s of retry budget before the window
#    closes.
#
# 2. Registry indexing lag — each publish takes several seconds to
#    become visible on the index CDN, blocking the next publish in
#    the chain. We poll the registry (up to 60s) between publishes.

set -euo pipefail

cd "$(dirname "$0")/.."

# Dependency-order list. Keep it in sync with
#   cargo metadata --no-deps | jq ...
CRATES=(
    obs-types
    obs-build
    obs-proto
    obs-core
    obs-macros
    obs-clickhouse
    obs-live-tail
    obs-otel
    obs-parquet
    obs-tower
    obs-tracing-bridge
    obs-sink-batch
    obs-prom
    obs-kit
    obs-cli
)

DRY_RUN=""
if [[ "${1:-}" == "--dry-run" ]]; then
    DRY_RUN="--dry-run"
    shift
fi

version_of() {
    local crate="$1"
    cargo metadata --format-version=1 --no-deps 2>/dev/null |
        python3 -c "import sys,json;md=json.load(sys.stdin);
print(next((p['version'] for p in md['packages'] if p['name']=='$crate'),''))"
}

already_published() {
    local crate="$1"
    local version="$2"
    local code
    code=$(curl -sS -o /dev/null -w "%{http_code}" \
        "https://crates.io/api/v1/crates/$crate/$version")
    [[ "$code" == "200" ]]
}

wait_for_index() {
    local crate="$1"
    local version="$2"
    # Poll the index until the new version is resolvable. Cap at 60s
    # — beyond that something is wrong and the operator should look.
    local deadline=$((SECONDS + 60))
    while (( SECONDS < deadline )); do
        if already_published "$crate" "$version"; then
            return 0
        fi
        sleep 2
    done
    echo "timeout waiting for $crate@$version to appear on crates.io" >&2
    return 1
}

# Parse the retry-after deadline out of cargo's stderr. crates.io's
# 429 body reads:
#   ... Please try again after Fri, 08 May 2026 04:30:26 GMT and see ...
# Returns the number of seconds to sleep (at least 1, capped implicitly
# by the timestamp). Emits 0 when no such timestamp is present.
seconds_until_retry() {
    local log_file="$1"
    python3 - "$log_file" <<'PY'
import re
import sys
import time
from email.utils import parsedate_to_datetime

log = open(sys.argv[1], encoding='utf-8', errors='replace').read()
# Accept "Please try again after <RFC-1123>" (documented shape) and
# the bare "try again after <date>" fallback.
m = re.search(r"try again after\s+([A-Z][a-z]{2},\s+\d{1,2}\s+[A-Z][a-z]{2}\s+\d{4}\s+\d{2}:\d{2}:\d{2}\s+GMT)", log)
if not m:
    print(0)
    sys.exit(0)
try:
    dt = parsedate_to_datetime(m.group(1))
except (TypeError, ValueError):
    print(0)
    sys.exit(0)
now = time.time()
# Sleep until the deadline + 10s jitter so we don't race the clock.
delta = max(1, int(dt.timestamp() - now) + 10)
print(delta)
PY
}

publish_one() {
    local crate="$1"
    local version
    version=$(version_of "$crate")
    if [[ -z "$version" ]]; then
        echo "could not resolve version for $crate" >&2
        exit 1
    fi

    if [[ -z "$DRY_RUN" ]] && already_published "$crate" "$version"; then
        echo "== $crate@$version already on crates.io — skipping"
        return 0
    fi

    echo "== publishing $crate@$version $DRY_RUN"

    # Retry on 429. The server tells us *when* to come back via the
    # "try again after <RFC-1123>" message in the 429 body; we parse
    # it out and sleep exactly that long. On non-429 failures fall
    # back to a small fixed delay (these are rare — usually network
    # hiccups).
    local attempt=1
    local max_attempts=6
    local log_file
    log_file=$(mktemp -t obs-publish.XXXXXX)
    trap 'rm -f "$log_file"' RETURN

    local published=0
    while (( attempt <= max_attempts )); do
        # Tee stderr so the operator sees progress, but also capture
        # it so we can parse the retry-after timestamp.
        if cargo publish -p "$crate" $DRY_RUN 2> >(tee "$log_file" >&2); then
            published=1
            break
        fi

        if (( attempt == max_attempts )); then
            echo "   ✗ $crate failed after $max_attempts attempts" >&2
            exit 1
        fi

        local sleep_for
        sleep_for=$(seconds_until_retry "$log_file")
        if (( sleep_for > 0 )); then
            local mins=$(( sleep_for / 60 ))
            local secs=$(( sleep_for % 60 ))
            echo "   429 rate-limit; sleeping ${mins}m${secs}s until server's retry-after..." >&2
        else
            sleep_for=30
            echo "   attempt $attempt failed (no retry-after hint); sleeping ${sleep_for}s..." >&2
        fi
        sleep "$sleep_for"
        attempt=$(( attempt + 1 ))
    done

    if (( published == 1 )) && [[ -z "$DRY_RUN" ]]; then
        echo "   waiting for crates.io to index $crate@$version..."
        wait_for_index "$crate" "$version"
        echo "   ✓ indexed"
    fi
}

for crate in "${CRATES[@]}"; do
    publish_one "$crate"
done

echo "done."
