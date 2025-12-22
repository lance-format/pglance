# lance extension development commands

# Default PostgreSQL version
pg_version := "16"
pgrx_version := "0.14.3"

# Show available commands
default:
    @just --list

ensure-pgrx:
    @set -euo pipefail; \
      if cargo pgrx --version 2>/dev/null | rg -q "cargo-pgrx {{pgrx_version}}"; then \
        true; \
      else \
        echo "Installing cargo-pgrx {{pgrx_version}} (required by Cargo.toml)"; \
        cargo install cargo-pgrx --version={{pgrx_version}} --locked; \
      fi; \
      if cargo pgrx info pg-config {{pg_version}} >/dev/null 2>&1; then \
        exit 0; \
      fi; \
      echo "pgrx Postgres {{pg_version}} is not initialized; running 'cargo pgrx init --pg{{pg_version}}=download'"; \
      cargo pgrx init --pg{{pg_version}}=download

# Run all quality checks
check pg=pg_version: ensure-pgrx
    cargo fmt --all -- --check
    cargo clippy --no-default-features --features pg{{pg}} -- -D warnings
    cargo test --no-default-features --features pg{{pg}}

# Auto-format all code
fmt:
    cargo fmt --all

# Run all tests (unit + integration)
test pg=pg_version: ensure-pgrx
    cargo test --no-default-features --features pg{{pg}}

# Build extension
build pg=pg_version: ensure-pgrx
    cargo pgrx package --pg-config "$(cargo pgrx info pg-config {{pg}})" --no-default-features --features pg{{pg}}

# Build release version
build-release pg=pg_version: ensure-pgrx
    cargo pgrx package --pg-config "$(cargo pgrx info pg-config {{pg}})" --no-default-features --features pg{{pg}} --release

# Install extension locally
install pg=pg_version: ensure-pgrx (build pg)
    cargo pgrx install --pg-config "$(cargo pgrx info pg-config {{pg}})" --features pg{{pg}}

# Run clippy linter
clippy pg=pg_version: ensure-pgrx
    cargo clippy --no-default-features --features pg{{pg}} -- -D warnings

# Setup development environment
setup:
    cargo install cargo-pgrx --version=0.14.3 --locked
    cargo pgrx init

# Clean build artifacts
clean:
    cargo clean

# Start PostgreSQL with extension
run pg=pg_version: ensure-pgrx (install pg)
    cargo pgrx stop pg{{pg}} >/dev/null 2>&1 || true
    cargo pgrx start pg{{pg}}
    just reload-ext {{pg}}

# Reload extension objects in the dev database (drop + create) to match the latest code.
reload-ext pg=pg_version: ensure-pgrx
    @set -euo pipefail; \
      pg_config="$(cargo pgrx info pg-config {{pg}})"; \
      psql_bin="$(dirname "$pg_config")/psql"; \
      port="$(sed -n '4p' "$HOME/.pgrx/data-{{pg}}/postmaster.pid" | tr -d '\n')"; \
      sockdir="$(sed -n '5p' "$HOME/.pgrx/data-{{pg}}/postmaster.pid" | tr -d '\n')"; \
      if "$psql_bin" -h "$sockdir" -p "$port" -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='pglance'" | rg -q "^1$"; then \
        true; \
      else \
        "$psql_bin" -h "$sockdir" -p "$port" -d postgres -v ON_ERROR_STOP=1 -c "CREATE DATABASE pglance"; \
      fi; \
      "$psql_bin" -h "$sockdir" -p "$port" -d pglance -v ON_ERROR_STOP=1 -c "DROP EXTENSION IF EXISTS pglance CASCADE; DROP EXTENSION IF EXISTS lance CASCADE; CREATE EXTENSION lance;"

# Security audit
audit:
    cargo audit

# Check for outdated dependencies
deps:
    cargo outdated

# Quick fix for common issues
fix:
    cargo fmt --all

# Simulate CI locally
# Run all quality checks (format + lint + test)
ci pg=pg_version:
    just ensure-pgrx
    cargo fmt --all -- --check
    cargo clippy --no-default-features --features pg{{pg}} -- -D warnings
    cargo test --no-default-features --features pg{{pg}}
    @echo "✅ All checks passed!"
