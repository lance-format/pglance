# pglance (PostgreSQL extension name: `lance`)

`pglance` is a PostgreSQL extension built with [pgrx](https://github.com/pgcentralfoundation/pgrx) that exposes a [Lance](https://lancedb.github.io/lance/) dataset as a PostgreSQL foreign table via an FDW, aiming for a native-like query experience.

> Note: The Rust crate/package is currently named `pglance`, but the PostgreSQL extension name is `lance` (i.e. you run `CREATE EXTENSION lance;`).

## Features

- Foreign Data Wrapper: `lance_fdw`
- Auto schema discovery + DDL: `lance_import(server, schema, table, uri, batch_size => NULL)`
- Native-first type mapping:
  - Scalars map to native PostgreSQL scalar types where possible
  - `list<T>` maps to `T[]` when possible
  - `struct{...}` maps to PostgreSQL composite types (created automatically during import)
  - `map<...>` currently falls back to `jsonb`

## Quick Start (PostgreSQL 16)

### Prerequisites

- Rust (stable)
- `protoc` (Protocol Buffers compiler)
- `cargo-pgrx` (must match the pinned `pgrx` version)

### Build and Run Locally

If you have [`just`](https://github.com/casey/just) installed:

```bash
just run
```

This starts a pgrx-managed PostgreSQL instance and reloads the extension to match the latest code.

Without `just`:

```bash
cargo install cargo-pgrx --version=0.14.3 --locked
cargo pgrx init --pg16=download

cargo pgrx install --features pg16
cargo pgrx run --features pg16 pg16
```

## Usage

### 1) Create the extension and server

```sql
CREATE EXTENSION lance;
CREATE SERVER lance_srv FOREIGN DATA WRAPPER lance_fdw;
```

### 2) Import a Lance dataset as a foreign table

```sql
SELECT lance_import(
  'lance_srv',
  'public',
  'my_lance_table',
  '/path/to/your/lance/table',
  batch_size => NULL
);
```

`lance_import` creates (if not already present):

- The foreign table `public.my_lance_table`
- Composite types for nested `struct` fields, e.g. `public.lance_my_lance_table_meta`

### 3) Query like a regular table

```sql
SELECT count(*) FROM public.my_lance_table;

SELECT * FROM public.my_lance_table LIMIT 10;
```

### 4) Attach and sync a Lance namespace

```sql
-- Plan only (no DDL).
SELECT *
  FROM lance_attach_namespace('lance_srv', dry_run => true);

-- Attach a namespace subtree into local schemas/tables.
SELECT *
  FROM lance_attach_namespace(
    'lance_srv',
    root_namespace_id => ARRAY[]::text[],
    schema_prefix => 'lance',
    batch_size => NULL,
    limit_per_list_call => 1000,
    dry_run => false
  );

-- Reconcile local objects with the remote namespace.
SELECT *
  FROM lance_sync_namespace(
    'lance_srv',
    schema_prefix => 'lance',
    drop_missing => false,
    recreate_changed => false,
    dry_run => true
  );
```

## Type Mapping (native-first)

| Arrow/Lance Type | PostgreSQL Type |
|------------------|-----------------|
| Boolean          | boolean         |
| Int8/UInt8       | int2            |
| Int16/UInt16     | int2            |
| Int32/UInt32     | int4            |
| Int64/UInt64     | int8            |
| Float16/Float32  | float4          |
| Float64          | float8          |
| Utf8/LargeUtf8   | text            |
| Binary           | bytea           |
| Date32/Date64    | date            |
| Timestamp        | timestamp / timestamptz |
| List             | array types     |
| Struct           | composite types |
| Map              | jsonb           |

## Development

Recommended workflow:

```bash
just ci
just run
```

Notes:

- `just reload-ext` drops and recreates the extension, so it will also drop dependent objects (e.g. foreign servers / foreign tables) and you may need to recreate them.
