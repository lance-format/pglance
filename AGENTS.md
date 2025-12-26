# Agent Guide (AGENTS.md)

This repository contains `pglance`, a Rust crate that builds a PostgreSQL extension named `lance` (pgrx).

## Goals

- Prefer small, reviewable patches that keep behavior stable unless explicitly requested.
- Prioritize correctness and safety (FDW behavior, type mapping, and DDL generation are user-facing).
- Avoid “drive-by” refactors and feature additions that are not required for the task.
- Implement behavior tests using `sqllogictest` scripts under `tests/sql/` whenever possible.

## Project Facts (easy to get wrong)

- Crate/package name: `pglance`
- PostgreSQL extension name: `lance` (use `CREATE EXTENSION lance;`)
- Pinned pgrx version: `0.14.3` (keep `cargo-pgrx` aligned)
- Default PostgreSQL feature: `pg16` (see `Cargo.toml`)

## Common Commands

Use `justfile` targets whenever possible:

- `just ci`  
  Runs `cargo fmt --check`, `cargo clippy -D warnings`, and `cargo test` (with `pg16`).
- `just run`  
  Installs the extension and starts a pgrx-managed Postgres instance, then reloads the extension.
- `just reload-ext`  
  Drops and recreates the extension (can drop dependent objects).

If `pgrx` is not initialized yet:

- `cargo pgrx init --pg16=download`

## Development Guardrails

- Do not change public SQL surface (function names, signatures, generated SQL) unless the task requires it.
- Be careful with `reload-ext`: it performs `DROP EXTENSION ... CASCADE`.
- Keep dependency changes minimal; prefer using existing crates already in `Cargo.toml`.
- Follow existing Rust style; keep the code `rustfmt`-clean and `clippy`-clean.

## Validation Checklist (before handing off)

- `just ci`
- If the change affects SQL/extension wiring: `just run`, then in psql:
  - `CREATE EXTENSION lance;`
  - `CREATE SERVER lance_srv FOREIGN DATA WRAPPER lance_fdw;`

## Code Layout

- `src/lib.rs`: extension entry points and pgrx exports
- `src/fdw/`: FDW implementation (options, import, scan, type mapping, conversions)
