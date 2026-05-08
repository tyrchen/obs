#!/usr/bin/env bash
# Publish every not-yet-published obs crate in dependency order.
#
# Usage:
#   scripts/publish-all.sh              # publish anything crates.io doesn't already have
#   scripts/publish-all.sh --dry-run    # dry-run each crate, skipping already-published
#
# crates.io serves from a CDN, so there's a short indexing lag after
# `cargo publish` before the new version appears in the index. This
# script polls the registry between publishes rather than sleeping a
# fixed interval, so fast networks don't pay for slow ones and vice
# versa.

set -euo pipefail

cd "$(dirname "$0")/.."

# Dependency-order list. Keep it in sync with
#   cargo metadata --no-deps | jq ... (see scripts/dep-order.py).
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

    # Retry with backoff on rate-limit / transient upload failures.
    # crates.io rate-limits new crate creation per account; 3 attempts
    # with 60s/120s backoff is enough to ride out a minute-window cap.
    local attempt=1
    local max_attempts=3
    local delay=60
    while (( attempt <= max_attempts )); do
        if cargo publish -p "$crate" $DRY_RUN; then
            break
        fi
        if (( attempt == max_attempts )); then
            echo "   ✗ $crate failed after $max_attempts attempts" >&2
            exit 1
        fi
        echo "   attempt $attempt failed; sleeping ${delay}s before retry..." >&2
        sleep "$delay"
        delay=$(( delay * 2 ))
        attempt=$(( attempt + 1 ))
    done

    if [[ -z "$DRY_RUN" ]]; then
        echo "   waiting for crates.io to index $crate@$version..."
        wait_for_index "$crate" "$version"
        echo "   ✓ indexed"
    fi
}

for crate in "${CRATES[@]}"; do
    publish_one "$crate"
done

echo "done."
