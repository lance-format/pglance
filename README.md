# pglance (PostgreSQL extension name: `lance`)

`pglance` is a PostgreSQL extension built with [pgrx](https://github.com/pgcentralfoundation/pgrx) that exposes [Lance](https://lancedb.github.io/lance/) datasets as PostgreSQL foreign tables via an FDW, and provides write operations (append, merge-insert, delete) to push data from PostgreSQL into Lance tables.

> Note: The Rust crate/package is currently named `pglance`, but the PostgreSQL extension name is `lance` (i.e. you run `CREATE EXTENSION lance;`).

## Features

### Read (FDW)

- Foreign Data Wrapper: `lance_fdw`
- Auto schema discovery + DDL: `lance_import(server, schema, table, uri, batch_size => NULL)`
- Namespace attach/sync: bulk-import entire Lance namespace trees
- Native-first type mapping:
  - Scalars map to native PostgreSQL scalar types where possible
  - `list<T>` maps to `T[]` when possible
  - `struct{...}` maps to PostgreSQL composite types (created automatically during import)
  - `map<...>` currently falls back to `jsonb`

### Write

- `lance_append()` — bulk-load rows from any SQL query into a Lance dataset (create / append / overwrite modes)
- `lance_merge_insert()` — upsert rows with match-on-key semantics (insert new, update existing)
- `lance_delete()` — remove rows matching a Lance predicate expression

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

### 5) Write data into Lance datasets

#### Append rows

```sql
SELECT * FROM lance_append(
    uri           := '/path/to/dataset.lance',
    source_query  := 'SELECT id, name, value FROM my_pg_table',
    mode          := 'create',     -- 'create' | 'append' | 'overwrite'
    batch_size    := 1024,
    server_name   := NULL          -- foreign server for S3 credentials
);
--  rows_written | duration_ms
-- --------------+-------------
--          5000 |         320
```

| Mode | Behaviour |
|---|---|
| `create` | Creates a new Lance dataset. Errors if it already exists. |
| `append` | Adds rows to an existing dataset. Errors if it doesn't exist. |
| `overwrite` | Replaces all data in an existing dataset. |

#### Upsert rows (merge-insert)

```sql
SELECT * FROM lance_merge_insert(
    uri              := '/path/to/dataset.lance',
    source_query     := 'SELECT id, name, email FROM staging.customers',
    on_columns       := ARRAY['id'],
    when_matched     := 'update',    -- 'update' | 'nothing'
    when_not_matched := 'insert',    -- 'insert' | 'nothing'
    batch_size       := 1024,
    server_name      := NULL
);
--  rows_merged | rows_inserted | rows_updated | duration_ms
-- -------------+---------------+--------------+-------------
--         1500 |               |              |        2340
```

> Note: The Lance SDK does not currently return separate insert/update counts, so `rows_inserted` and `rows_updated` may be NULL.

#### Delete rows by predicate

```sql
SELECT * FROM lance_delete(
    uri         := '/path/to/dataset.lance',
    predicate   := 'id > 100 AND status = ''old''',
    server_name := NULL
);
--  fragments_removed | duration_ms
-- -------------------+-------------
--                  3 |         120
```

The `predicate` uses Lance's own expression syntax (a subset of SQL). Column names must match the Lance schema.

#### S3 credentials via foreign server

```sql
-- Reuse credentials from a lance_fdw foreign server
SELECT * FROM lance_append(
    's3://my-bucket/lakehouse/events.lance',
    'SELECT * FROM staging.events',
    server_name := 'my_lance_server'
);
```

## Type Mapping

### Read path (Arrow → PostgreSQL)

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

### Write path (PostgreSQL → Arrow)

| PostgreSQL Type | Arrow Type |
|---|---|
| bool | Boolean |
| int2 | Int16 |
| int4 | Int32 |
| int8 | Int64 |
| float4 | Float32 |
| float8 | Float64 |
| numeric | Utf8 (string representation) |
| text / varchar | Utf8 |
| bytea | Binary |
| date | Date32 |
| timestamp | Timestamp(Microsecond, None) |
| timestamptz | Timestamp(Microsecond, UTC) |
| jsonb | Utf8 (serialised JSON) |

## Development

Recommended workflow:

```bash
just ci       # format check + clippy + tests
just run      # build, install, start PG, reload extension
just test     # run unit + SLT integration tests
```

### Running tests

Tests use the [sqllogictest](https://github.com/risinglightdb/sqllogictest-rs) framework. Test files live in `tests/sql/*.slt`.

```bash
just test
# or explicitly:
cargo test --no-default-features --features pg16
```

### End-to-end test with DuckDB verification

A standalone SQL script exercises every write operation and cross-verifies each mutation by reading the Lance files independently through DuckDB's `lance_scan`.

**Prerequisites:** PostgreSQL with both the `lance` and `duckdb` extensions installed.

```bash
psql -v lance_dir='/tmp/lance_e2e_test' -f tests/e2e_lance_duckdb.sql
```

Notes:

- `just reload-ext` drops and recreates the extension, so it will also drop dependent objects (e.g. foreign servers / foreign tables) and you may need to recreate them.
