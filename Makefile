build:
	@cargo build

test:
	@cargo nextest run --all-features

release:
	@cargo release tag --execute
	@git cliff -o CHANGELOG.md
	@git commit -a -n -m "Update CHANGELOG.md" || true
	@git push origin master
	@cargo release push --execute

update-submodule:
	@git submodule update --init --recursive --remote

# ─── Phase 5 hardening targets ────────────────────────────────────────

# Strict-superset clippy run used as the M4 exit criterion. Spec 90 § M4
# / impl-plan 5.3.
#
# We feed the workspace through pedantic with a curated set of allows.
# The list lives in `scripts/pedantic-allows.txt` so the Makefile stays
# readable; each entry is a stylistic lint that the team has decided
# is not load-bearing for the project's quality contract (cast_*,
# doc_markdown, module_name_repetitions, ...). Lints that surface
# real correctness issues (the existing `unwrap_used` / `panic` /
# `indexing_slicing` set in `[workspace.lints.clippy]`) stay on.
lint-strict:
	@bash scripts/lint-strict.sh

# Audit dependencies for known security advisories.
#
# We delegate to `cargo deny check advisories` rather than `cargo audit`
# directly: deny consumes the same RustSec advisory database and
# tolerates the CVSS 4.0 advisory format, while older `cargo audit`
# binaries still error on CVSS 4.0. Spec 90 § M4 / impl-plan 5.3.
audit:
	@cargo deny check advisories

# Enforce license + ban policies via cargo-deny (advisories + bans +
# licenses + sources). Spec 90 § M4 / impl-plan 5.3.
deny:
	@cargo deny check

# Short CI smoke variant of the soak harness — runs ~30 seconds at
# 50k events/sec into a NonBlockingWriter-wrapped NDJSON sink with all
# locally-runnable sinks active. Asserts ObsSinkDropped == 0 after the
# warm-up window. Spec 90 § M4 / impl-plan 5.1 + 5.2.
soak:
	@cargo run --release -p obs-soak -- --duration 30 --rate 50000 --warmup-secs 5 --sample-secs 5

# Full 24-hour soak. Run before stamping v1.0. Spec 90 § M4 /
# impl-plan 5.1.
soak-24h:
	@cargo run --release -p obs-soak -- --duration 86400 --rate 50000 --warmup-secs 60 --sample-secs 300

# Free-running ceiling probe — measures the SDK's emit pipeline ceiling
# on the local host without IO contention. Useful when tuning queue
# defaults or comparing benchmarks across hardware.
soak-ceiling:
	@cargo run --release -p obs-soak -- --duration 10 --rate 200000 --null-sink --unbounded --warmup-secs 1 --sample-secs 2

# Verifies the envelope wire shape stays at format_ver = 1 across this
# diff range. Spec 90 § 3.3 / impl-plan 5.5.
check-format-ver:
	@bash scripts/check-format-ver.sh

.PHONY: build test release update-submodule lint-strict audit deny soak soak-24h soak-ceiling check-format-ver
