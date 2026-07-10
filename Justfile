# ChartPDS task orchestration. Run `just --list` to see all recipes.

# Default: show available recipes.
default:
    @just --list

# Run the full check pipeline (format, lint, type-check, test, holdout lockfile
# verify, deny, machete, and verify the sqlx offline cache is in sync with the
# current migrations).
check: _verify-tools _check-sql fmt-check lint junk-types typecheck test holdout-verify deny machete

# Verify the .sqlx/ offline cache matches the current schema + queries. Drops
# and rebuilds a temporary SQLite from migrations, then `cargo sqlx prepare
# --check` validates the committed cache without writing changes. Catches
# "forgot to run just prepare-sql" at lint time rather than at runtime.
[private]
_check-sql:
    @mkdir -p target/sqlx
    @rm -f target/sqlx/build.db
    DATABASE_URL=sqlite://target/sqlx/build.db?mode=rwc \
        cargo sqlx migrate run --source crates/chartpds-core/migrations
    DATABASE_URL=sqlite://target/sqlx/build.db?mode=rwc \
        cargo sqlx prepare --workspace --check -- --all-targets

# Verify required cargo subcommands are installed before running check.
[private]
_verify-tools:
    @cargo deny --version >/dev/null 2>&1 || { echo "Missing cargo-deny. Run 'just install-tools' first." >&2; exit 1; }
    @cargo machete --version >/dev/null 2>&1 || { echo "Missing cargo-machete. Run 'just install-tools' first." >&2; exit 1; }
    @cargo sqlx --version >/dev/null 2>&1 || { echo "Missing sqlx-cli. Run 'just install-tools' first." >&2; exit 1; }

# Check formatting without modifying files.
fmt-check:
    cargo fmt --check

# Format all files.
fmt:
    cargo fmt

# Run clippy with the aggressive workspace profile.
lint:
    cargo clippy --workspace --all-targets -- -D warnings

# Forbid junk parameter-object types (struct *Params/*Options, core *Args).
# See "No junk parameter types" in CLAUDE.md.
junk-types:
    ./scripts/forbid-junk-types.sh

# Type-check without compiling artifacts.
typecheck:
    cargo check --workspace --all-targets

# Run the test suite. Build the chartpds-mcp binary first: the holdout suite
# spawns it as a child process and locates it at target/<profile>/chartpds-mcp.
# A bare `cargo test --workspace` does not emit that standalone artifact for a
# sibling crate's tests, so the holdout harness would not find it on a clean
# build (e.g. in CI). Building it explicitly makes the suite hermetic.
test:
    cargo build -p chartpds-mcp
    cargo test --workspace

# Build the workspace.
build:
    cargo build --workspace

# Audit dependencies (licenses, advisories, banned crates).
deny:
    cargo deny check

# Detect unused dependencies in Cargo.toml files.
machete:
    cargo machete

# Install the cargo subcommands used by `just check` and the sqlx workflow.
install-tools:
    cargo install cargo-deny --locked
    cargo install cargo-machete --locked
    cargo install sqlx-cli --locked --no-default-features --features sqlite,rustls

# Regenerate the sqlx offline cache (.sqlx/query-*.json) after migration or
# query changes. Drops and rebuilds a build-time SQLite from the migrations,
# then asks sqlx-cli to capture every query's schema at the current state.
# The `-- --all-targets` tail forwards cargo's --all-targets so test-only
# queries (sqlx::query! in #[cfg(test)] blocks) are captured too.
prepare-sql:
    @mkdir -p target/sqlx
    @rm -f target/sqlx/build.db
    DATABASE_URL=sqlite://target/sqlx/build.db?mode=rwc \
        cargo sqlx migrate run --source crates/chartpds-core/migrations
    DATABASE_URL=sqlite://target/sqlx/build.db?mode=rwc \
        cargo sqlx prepare --workspace -- --all-targets

# Verify the holdout suite has not drifted from its lockfile. Part of `check`.
# This is the content half of the holdout gate; CI additionally enforces that
# protected-path changes are human-signed (see scripts/holdout-verify-signatures.sh).
holdout-verify:
    ./scripts/holdout-verify.sh

# Bless a deliberate change to the holdout suite: regenerate holdout.lock from
# the current holdout/ tree and commit it together with the holdout changes in a
# SIGNED commit. Only a human with the Touch-ID/Secure-Enclave signing key can
# complete the commit, which is exactly what gates the protected paths. Usage:
#   just holdout-bless "bless: add diabetes problem-list regression"
holdout-bless message:
    ./scripts/holdout-hashes.sh > holdout.lock
    git add holdout holdout.lock .github/allowed_signers .github/workflows/holdout.yml
    git commit -S -m "{{message}}"
