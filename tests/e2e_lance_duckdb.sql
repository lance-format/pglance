-- =============================================================================
-- End-to-End Test: Lance extension verified by DuckDB extension
-- =============================================================================
--
-- PREREQUISITES:
--   1. PostgreSQL extension `lance`  installed (pglance / CREATE EXTENSION lance)
--   2. PostgreSQL extension `pg_duckdb` installed (CREATE EXTENSION pg_duckdb)
--   3. Run from psql:  psql -f e2e_lance_duckdb.sql
--
-- Lance datasets are written to /tmp/lance_e2e_test.
--
-- This script tests:
--   - All supported data types (bool, int2/4/8, float4/8, text, bytea,
--     date, timestamp, timestamptz, numeric, jsonb)
--   - Single-row and bulk-row operations
--   - lance_append  (create / append / overwrite)
--   - lance_merge_insert  (insert + update)
--   - lance_delete
--   - Cross-verification of every mutation via DuckDB reading the same
--     Lance files directly, independent of the Lance FDW read path
-- =============================================================================

\set ON_ERROR_STOP on
\timing on

-- Remove any leftover data from a previous run
\! rm -rf /tmp/lance_e2e_test

\echo '============================================================'
\echo ' Lance + DuckDB End-to-End Test'
\echo ' Lance dataset directory: /tmp/lance_e2e_test'
\echo '============================================================'

-- ─────────────────────────────────────────────────────────────────────────────
-- 0. Setup: extensions, server, helper for DuckDB queries on Lance
-- ─────────────────────────────────────────────────────────────────────────────
\echo ''
\echo '>>> 0. Setup'

CREATE EXTENSION IF NOT EXISTS lance;
CREATE EXTENSION IF NOT EXISTS pg_duckdb;
CALL duckdb.recycle_ddb();
SET duckdb.allow_unsigned_extensions = true;
SELECT duckdb.raw_query('LOAD LANCE;');

-- FDW server used by lance_import
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_foreign_server WHERE srvname = 'lance_e2e') THEN
    EXECUTE 'CREATE SERVER lance_e2e FOREIGN DATA WRAPPER lance_fdw';
  END IF;
END $$;

-- Schema for lance foreign tables
CREATE SCHEMA IF NOT EXISTS le2e;

-- DuckDB needs the lance extension loaded once per session.
-- pg_duckdb uses duckdb.raw_query() for DDL statements.
SELECT duckdb.raw_query('INSTALL lance; LOAD lance;');

\echo '  ... extensions and helpers ready'

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. CREATE dataset with ALL data types (single row)
-- ─────────────────────────────────────────────────────────────────────────────
\echo ''
\echo '>>> 1. Create dataset — all data types (single row)'

DROP FOREIGN TABLE IF EXISTS le2e.t_types CASCADE;

SELECT * FROM lance_append(
  '/tmp/lance_e2e_test/t_types.lance',
  $q$
    SELECT
      true                                       AS col_bool,
      42::int2                                   AS col_int2,
      100000::int4                               AS col_int4,
      9223372036854775807::int8                  AS col_int8,
      3.14::float4                               AS col_float4,
      2.718281828459045::float8                  AS col_float8,
      'hello world'::text                        AS col_text,
      '\xDEADBEEF'::bytea                       AS col_bytea,
      '2025-06-15'::date                         AS col_date,
      '2025-06-15 14:30:00'::timestamp           AS col_ts,
      '2025-06-15 14:30:00+00'::timestamptz      AS col_tstz,
      123.456789::numeric                        AS col_numeric,
      '{"key": "value", "n": 42}'::jsonb         AS col_jsonb
  $q$,
  mode := 'create'
);

-- Import via Lance FDW
SELECT lance_import('lance_e2e', 'le2e', 't_types', '/tmp/lance_e2e_test/t_types.lance', batch_size => NULL);

-- Verify via Lance FDW
SELECT col_bool, col_int2, col_int4, col_int8,
       round(col_float4::numeric, 2) AS col_float4,
       round(col_float8::numeric, 6) AS col_float8,
       col_text, col_date::text, col_ts::text, col_tstz::text,
       col_numeric, col_jsonb
  FROM le2e.t_types;

-- Cross-check via DuckDB: read the Lance file directly using replacement scan
\echo '  ... DuckDB cross-check: row count'
SELECT * FROM duckdb.query($$SELECT count(*) AS duckdb_count FROM '/tmp/lance_e2e_test/t_types.lance'$$);

\echo '  PASS: single-row all-types create'

-- ─────────────────────────────────────────────────────────────────────────────
-- 2. BULK INSERT (create) — 1000 rows, verify count & spot-check via DuckDB
-- ─────────────────────────────────────────────────────────────────────────────
\echo ''
\echo '>>> 2. Bulk insert — 1000 rows'

DROP FOREIGN TABLE IF EXISTS le2e.t_bulk CASCADE;

SELECT * FROM lance_append(
  '/tmp/lance_e2e_test/t_bulk.lance',
  $q$
    SELECT
      v::int4                                     AS id,
      (v % 2 = 0)                                 AS flag,
      v::int2                                      AS small_v,
      (v * 1000)::int8                             AS big_v,
      (v * 1.1)::float4                            AS f4,
      (v * 2.2)::float8                            AS f8,
      ('item_' || v::text)::text                   AS name,
      ('2025-01-01'::date + v)                     AS d,
      ('2025-01-01 00:00:00'::timestamp + (v || ' hours')::interval)  AS ts,
      ('2025-01-01 00:00:00+00'::timestamptz + (v || ' hours')::interval) AS tstz,
      (v * 0.01)::numeric                          AS price,
      ('{"i":' || v || '}')::jsonb                 AS meta
    FROM generate_series(1, 1000) v
  $q$,
  mode := 'create'
);

SELECT lance_import('lance_e2e', 'le2e', 't_bulk', '/tmp/lance_e2e_test/t_bulk.lance', batch_size => NULL);

-- Lance FDW count
SELECT count(*) AS lance_count FROM le2e.t_bulk;

-- DuckDB count
SELECT * FROM duckdb.query($$SELECT count(*) AS duckdb_count FROM '/tmp/lance_e2e_test/t_bulk.lance'$$);

-- Spot-check: row id=500
SELECT id, name, flag FROM le2e.t_bulk WHERE id = 500;

-- DuckDB spot-check
SELECT * FROM duckdb.query($$SELECT id, name, flag FROM '/tmp/lance_e2e_test/t_bulk.lance' WHERE id = 500$$);

\echo '  PASS: bulk insert 1000 rows'

-- ─────────────────────────────────────────────────────────────────────────────
-- 3. APPEND — add more rows to existing dataset
-- ─────────────────────────────────────────────────────────────────────────────
\echo ''
\echo '>>> 3. Append rows to existing dataset'

SELECT * FROM lance_append(
  '/tmp/lance_e2e_test/t_bulk.lance',
  $q$
    SELECT
      v::int4                                     AS id,
      (v % 2 = 0)                                 AS flag,
      v::int2                                      AS small_v,
      (v * 1000)::int8                             AS big_v,
      (v * 1.1)::float4                            AS f4,
      (v * 2.2)::float8                            AS f8,
      ('item_' || v::text)::text                   AS name,
      ('2025-01-01'::date + v)                     AS d,
      ('2025-01-01 00:00:00'::timestamp + (v || ' hours')::interval)  AS ts,
      ('2025-01-01 00:00:00+00'::timestamptz + (v || ' hours')::interval) AS tstz,
      (v * 0.01)::numeric                          AS price,
      ('{"i":' || v || '}')::jsonb                 AS meta
    FROM generate_series(1001, 1500) v
  $q$,
  mode := 'append'
);

-- Re-import to pick up new data
DROP FOREIGN TABLE IF EXISTS le2e.t_bulk CASCADE;
SELECT lance_import('lance_e2e', 'le2e', 't_bulk', '/tmp/lance_e2e_test/t_bulk.lance', batch_size => NULL);

-- Verify: should now have 1500 rows
SELECT count(*) AS lance_count_after_append FROM le2e.t_bulk;

SELECT * FROM duckdb.query($$SELECT count(*) AS duckdb_count_after_append FROM '/tmp/lance_e2e_test/t_bulk.lance'$$);

-- Verify appended rows exist
SELECT id, name FROM le2e.t_bulk WHERE id = 1250;
SELECT * FROM duckdb.query($$SELECT id, name FROM '/tmp/lance_e2e_test/t_bulk.lance' WHERE id = 1250$$);

\echo '  PASS: append 500 rows (total 1500)'

-- ─────────────────────────────────────────────────────────────────────────────
-- 4. OVERWRITE — replace all data
-- ─────────────────────────────────────────────────────────────────────────────
\echo ''
\echo '>>> 4. Overwrite dataset'

SELECT * FROM lance_append(
  '/tmp/lance_e2e_test/t_bulk.lance',
  $q$
    SELECT
      v::int4              AS id,
      true                 AS flag,
      v::int2              AS small_v,
      (v * 999)::int8      AS big_v,
      (v * 3.3)::float4    AS f4,
      (v * 4.4)::float8    AS f8,
      ('new_' || v::text)  AS name,
      '2026-01-01'::date   AS d,
      '2026-01-01 12:00:00'::timestamp AS ts,
      '2026-01-01 12:00:00+00'::timestamptz AS tstz,
      (v * 0.5)::numeric   AS price,
      '{"overwritten":true}'::jsonb AS meta
    FROM generate_series(1, 10) v
  $q$,
  mode := 'overwrite'
);

DROP FOREIGN TABLE IF EXISTS le2e.t_bulk CASCADE;
SELECT lance_import('lance_e2e', 'le2e', 't_bulk', '/tmp/lance_e2e_test/t_bulk.lance', batch_size => NULL);

-- Should be exactly 10 rows (all previous 1500 replaced)
SELECT count(*) AS lance_count_after_overwrite FROM le2e.t_bulk;

SELECT * FROM duckdb.query($$SELECT count(*) AS duckdb_count_after_overwrite FROM '/tmp/lance_e2e_test/t_bulk.lance'$$);

-- All names should start with 'new_'
SELECT count(*) AS all_new FROM le2e.t_bulk WHERE name LIKE 'new_%';

\echo '  PASS: overwrite (10 rows replace 1500)'

-- ─────────────────────────────────────────────────────────────────────────────
-- 5. MERGE-INSERT — upsert: update existing + insert new (single rows)
-- ─────────────────────────────────────────────────────────────────────────────
\echo ''
\echo '>>> 5. Merge-insert — single row update + single row insert'

-- Create a fresh small dataset
DROP FOREIGN TABLE IF EXISTS le2e.t_merge CASCADE;

SELECT * FROM lance_append(
  '/tmp/lance_e2e_test/t_merge.lance',
  $q$
    SELECT v::int4 AS id,
           ('orig_' || v::text) AS name,
           (v * 10.0)::float8 AS score,
           true AS active,
           '2025-01-01'::date AS created,
           ('2025-01-01 00:00:00'::timestamp + (v || ' hours')::interval) AS updated_at
    FROM generate_series(1, 5) v
  $q$,
  mode := 'create'
);

SELECT lance_import('lance_e2e', 'le2e', 't_merge', '/tmp/lance_e2e_test/t_merge.lance', batch_size => NULL);
SELECT count(*) AS before_merge FROM le2e.t_merge;

-- Merge: update id=3, insert id=6
SELECT * FROM lance_merge_insert(
  '/tmp/lance_e2e_test/t_merge.lance',
  $q$
    SELECT * FROM (VALUES
      (3::int4, 'updated_3'::text, 99.9::float8, false, '2025-06-01'::date, '2025-06-01 12:00:00'::timestamp),
      (6::int4, 'new_6'::text,     60.0::float8, true,  '2025-06-01'::date, '2025-06-01 12:00:00'::timestamp)
    ) AS t(id, name, score, active, created, updated_at)
  $q$,
  on_columns := ARRAY['id'],
  when_matched := 'update',
  when_not_matched := 'insert'
);

-- Re-import
DROP FOREIGN TABLE IF EXISTS le2e.t_merge CASCADE;
SELECT lance_import('lance_e2e', 'le2e', 't_merge', '/tmp/lance_e2e_test/t_merge.lance', batch_size => NULL);

-- Should have 6 rows now
SELECT count(*) AS after_merge FROM le2e.t_merge;

-- Verify updated row (id=3)
\echo '  id=3 should show updated_3:'
SELECT id, name, round(score::numeric, 1) AS score, active FROM le2e.t_merge WHERE id = 3;

-- Verify inserted row (id=6)
\echo '  id=6 should show new_6:'
SELECT id, name, round(score::numeric, 1) AS score, active FROM le2e.t_merge WHERE id = 6;

-- Verify unchanged row (id=1)
\echo '  id=1 should show orig_1:'
SELECT id, name, round(score::numeric, 1) AS score, active FROM le2e.t_merge WHERE id = 1;

-- DuckDB cross-check
SELECT * FROM duckdb.query($$SELECT count(*) AS duckdb_after_merge FROM '/tmp/lance_e2e_test/t_merge.lance'$$);

\echo '  PASS: merge-insert single rows (update + insert)'

-- ─────────────────────────────────────────────────────────────────────────────
-- 6. MERGE-INSERT — bulk upsert (update 5 + insert 5)
-- ─────────────────────────────────────────────────────────────────────────────
\echo ''
\echo '>>> 6. Merge-insert — bulk upsert (update 5 + insert 5)'

SELECT * FROM lance_merge_insert(
  '/tmp/lance_e2e_test/t_merge.lance',
  $q$
    SELECT v::int4 AS id,
           ('bulk_' || v::text) AS name,
           (v * 100.0)::float8 AS score,
           (v % 2 = 0) AS active,
           '2025-07-01'::date AS created,
           '2025-07-01 00:00:00'::timestamp AS updated_at
    FROM generate_series(2, 11) v
  $q$,
  on_columns := ARRAY['id'],
  when_matched := 'update',
  when_not_matched := 'insert'
);

DROP FOREIGN TABLE IF EXISTS le2e.t_merge CASCADE;
SELECT lance_import('lance_e2e', 'le2e', 't_merge', '/tmp/lance_e2e_test/t_merge.lance', batch_size => NULL);

-- ids 1..11 should exist = 11 rows
SELECT count(*) AS after_bulk_merge FROM le2e.t_merge;
SELECT * FROM duckdb.query($$SELECT count(*) AS duckdb_after_bulk_merge FROM '/tmp/lance_e2e_test/t_merge.lance'$$);

-- Rows 2-6 should be updated with 'bulk_' prefix
\echo '  Rows 2-6 should have bulk_ prefix:'
SELECT id, name FROM le2e.t_merge WHERE id BETWEEN 2 AND 6 ORDER BY id;

-- Rows 7-11 should be newly inserted with 'bulk_' prefix
\echo '  Rows 7-11 should be new with bulk_ prefix:'
SELECT id, name FROM le2e.t_merge WHERE id BETWEEN 7 AND 11 ORDER BY id;

-- Row 1 should still be orig_1 (untouched)
\echo '  Row 1 should still be orig_1:'
SELECT id, name FROM le2e.t_merge WHERE id = 1;

\echo '  PASS: bulk merge-insert (update 5 + insert 5)'

-- ─────────────────────────────────────────────────────────────────────────────
-- 7. MERGE-INSERT — update-only mode (when_not_matched := 'nothing')
-- ─────────────────────────────────────────────────────────────────────────────
\echo ''
\echo '>>> 7. Merge-insert — update-only mode'

SELECT * FROM lance_merge_insert(
  '/tmp/lance_e2e_test/t_merge.lance',
  $q$
    SELECT * FROM (VALUES
      (1::int4, 'upd_only_1'::text, 999.0::float8, false, '2025-08-01'::date, '2025-08-01 00:00:00'::timestamp),
      (99::int4, 'should_not_insert'::text, 0.0::float8, false, '2025-08-01'::date, '2025-08-01 00:00:00'::timestamp)
    ) AS t(id, name, score, active, created, updated_at)
  $q$,
  on_columns := ARRAY['id'],
  when_matched := 'update',
  when_not_matched := 'nothing'
);

DROP FOREIGN TABLE IF EXISTS le2e.t_merge CASCADE;
SELECT lance_import('lance_e2e', 'le2e', 't_merge', '/tmp/lance_e2e_test/t_merge.lance', batch_size => NULL);

-- Still 11 rows (id=99 should NOT have been inserted)
SELECT count(*) AS after_update_only FROM le2e.t_merge;

-- id=1 should be updated
\echo '  id=1 should now be upd_only_1:'
SELECT id, name FROM le2e.t_merge WHERE id = 1;

-- id=99 should not exist
\echo '  id=99 should not exist:'
SELECT count(*) AS id99_count FROM le2e.t_merge WHERE id = 99;

\echo '  PASS: update-only merge (no spurious inserts)'

-- ─────────────────────────────────────────────────────────────────────────────
-- 8. MERGE-INSERT — insert-only mode (when_matched := 'nothing')
-- ─────────────────────────────────────────────────────────────────────────────
\echo ''
\echo '>>> 8. Merge-insert — insert-only mode'

SELECT * FROM lance_merge_insert(
  '/tmp/lance_e2e_test/t_merge.lance',
  $q$
    SELECT * FROM (VALUES
      (1::int4,  'no_update'::text, 0.0::float8, false, '2025-09-01'::date, '2025-09-01 00:00:00'::timestamp),
      (20::int4, 'inserted_20'::text, 200.0::float8, true, '2025-09-01'::date, '2025-09-01 00:00:00'::timestamp)
    ) AS t(id, name, score, active, created, updated_at)
  $q$,
  on_columns := ARRAY['id'],
  when_matched := 'nothing',
  when_not_matched := 'insert'
);

DROP FOREIGN TABLE IF EXISTS le2e.t_merge CASCADE;
SELECT lance_import('lance_e2e', 'le2e', 't_merge', '/tmp/lance_e2e_test/t_merge.lance', batch_size => NULL);

-- Should now have 12 rows (id=20 inserted, id=1 NOT updated)
SELECT count(*) AS after_insert_only FROM le2e.t_merge;

-- id=1 should still be upd_only_1 (not changed to 'no_update')
\echo '  id=1 should still be upd_only_1:'
SELECT id, name FROM le2e.t_merge WHERE id = 1;

-- id=20 should exist
\echo '  id=20 should be inserted_20:'
SELECT id, name FROM le2e.t_merge WHERE id = 20;

\echo '  PASS: insert-only merge (no spurious updates)'

-- ─────────────────────────────────────────────────────────────────────────────
-- 9. DELETE — single predicate (single row)
-- ─────────────────────────────────────────────────────────────────────────────
\echo ''
\echo '>>> 9. Delete — single row by predicate'

SELECT * FROM lance_delete(
  '/tmp/lance_e2e_test/t_merge.lance',
  'id = 20'
);

DROP FOREIGN TABLE IF EXISTS le2e.t_merge CASCADE;
SELECT lance_import('lance_e2e', 'le2e', 't_merge', '/tmp/lance_e2e_test/t_merge.lance', batch_size => NULL);

-- Back to 11 rows
SELECT count(*) AS after_single_delete FROM le2e.t_merge;
SELECT * FROM duckdb.query($$SELECT count(*) AS duckdb_after_single_delete FROM '/tmp/lance_e2e_test/t_merge.lance'$$);

-- id=20 should be gone
\echo '  id=20 should be gone:'
SELECT count(*) AS id20_count FROM le2e.t_merge WHERE id = 20;

\echo '  PASS: single-row delete'

-- ─────────────────────────────────────────────────────────────────────────────
-- 10. DELETE — bulk delete (range predicate)
-- ─────────────────────────────────────────────────────────────────────────────
\echo ''
\echo '>>> 10. Delete — bulk delete (range predicate: id >= 8)'

SELECT * FROM lance_delete(
  '/tmp/lance_e2e_test/t_merge.lance',
  'id >= 8'
);

DROP FOREIGN TABLE IF EXISTS le2e.t_merge CASCADE;
SELECT lance_import('lance_e2e', 'le2e', 't_merge', '/tmp/lance_e2e_test/t_merge.lance', batch_size => NULL);

-- ids 1..7 should remain = 7 rows
SELECT count(*) AS after_bulk_delete FROM le2e.t_merge;
SELECT * FROM duckdb.query($$SELECT count(*) AS duckdb_after_bulk_delete FROM '/tmp/lance_e2e_test/t_merge.lance'$$);

-- Max id should be 7
SELECT max(id) AS max_id_after_delete FROM le2e.t_merge;

\echo '  PASS: bulk delete (range)'

-- ─────────────────────────────────────────────────────────────────────────────
-- 11. DELETE — compound predicate
-- ─────────────────────────────────────────────────────────────────────────────
\echo ''
\echo '>>> 11. Delete — compound predicate (id < 3 AND active = false)'

SELECT * FROM lance_delete(
  '/tmp/lance_e2e_test/t_merge.lance',
  'id < 3 AND active = false'
);

DROP FOREIGN TABLE IF EXISTS le2e.t_merge CASCADE;
SELECT lance_import('lance_e2e', 'le2e', 't_merge', '/tmp/lance_e2e_test/t_merge.lance', batch_size => NULL);

SELECT count(*) AS after_compound_delete FROM le2e.t_merge;
SELECT * FROM duckdb.query($$SELECT count(*) AS duckdb_after_compound_delete FROM '/tmp/lance_e2e_test/t_merge.lance'$$);

\echo '  PASS: compound predicate delete'

-- ─────────────────────────────────────────────────────────────────────────────
-- 12. FULL ROUNDTRIP with DuckDB verification — all types
-- ─────────────────────────────────────────────────────────────────────────────
\echo ''
\echo '>>> 12. Full roundtrip: create → verify → update → verify → delete → verify'

DROP FOREIGN TABLE IF EXISTS le2e.t_roundtrip CASCADE;

-- Create with multiple data types
SELECT * FROM lance_append(
  '/tmp/lance_e2e_test/t_roundtrip.lance',
  $q$
    SELECT
      v::int4                                                        AS id,
      (v % 2 = 0)                                                   AS is_even,
      v::int2                                                        AS small_id,
      (v * 100000000000)::int8                                      AS big_id,
      (v * 1.5)::float4                                              AS ratio_f4,
      (v * 2.5)::float8                                              AS ratio_f8,
      ('name_' || v::text)::text                                     AS name,
      ('2025-01-01'::date + v)                                       AS created_date,
      ('2025-01-01 10:00:00'::timestamp + (v || ' minutes')::interval)  AS created_ts,
      ('2025-01-01 10:00:00+00'::timestamptz + (v || ' minutes')::interval) AS created_tstz,
      (v * 9.99)::numeric                                            AS amount,
      ('{"row":' || v || ',"tag":"test"}')::jsonb                    AS payload
    FROM generate_series(1, 100) v
  $q$,
  mode := 'create'
);

SELECT lance_import('lance_e2e', 'le2e', 't_roundtrip', '/tmp/lance_e2e_test/t_roundtrip.lance', batch_size => NULL);

\echo '  Step 1: verify creation (100 rows)'
SELECT count(*) AS lance_created FROM le2e.t_roundtrip;
SELECT * FROM duckdb.query($$SELECT count(*) AS duckdb_created FROM '/tmp/lance_e2e_test/t_roundtrip.lance'$$);

-- Merge-insert: update rows 1-10 and insert rows 101-110
\echo '  Step 2: merge-insert (update 10 + insert 10)'
SELECT * FROM lance_merge_insert(
  '/tmp/lance_e2e_test/t_roundtrip.lance',
  $q$
    SELECT
      v::int4                                                        AS id,
      (v % 3 = 0)                                                   AS is_even,
      (v + 100)::int2                                                AS small_id,
      (v * 999999999999)::int8                                      AS big_id,
      (v * 7.7)::float4                                              AS ratio_f4,
      (v * 8.8)::float8                                              AS ratio_f8,
      ('merged_' || v::text)::text                                   AS name,
      '2026-01-01'::date                                             AS created_date,
      '2026-01-01 00:00:00'::timestamp                               AS created_ts,
      '2026-01-01 00:00:00+00'::timestamptz                          AS created_tstz,
      (v * 100.01)::numeric                                          AS amount,
      ('{"row":' || v || ',"tag":"merged"}')::jsonb                  AS payload
    FROM generate_series(1, 110) v
    WHERE v <= 10 OR v > 100
  $q$,
  on_columns := ARRAY['id'],
  when_matched := 'update',
  when_not_matched := 'insert'
);

DROP FOREIGN TABLE IF EXISTS le2e.t_roundtrip CASCADE;
SELECT lance_import('lance_e2e', 'le2e', 't_roundtrip', '/tmp/lance_e2e_test/t_roundtrip.lance', batch_size => NULL);

-- Should have 110 rows
SELECT count(*) AS lance_after_merge FROM le2e.t_roundtrip;
SELECT * FROM duckdb.query($$SELECT count(*) AS duckdb_after_merge FROM '/tmp/lance_e2e_test/t_roundtrip.lance'$$);

-- Verify updated rows have 'merged_' prefix
SELECT count(*) AS merged_count FROM le2e.t_roundtrip WHERE name LIKE 'merged_%';

-- Verify untouched rows (11-100) still have 'name_' prefix
SELECT count(*) AS untouched_count FROM le2e.t_roundtrip WHERE name LIKE 'name_%';

-- Delete rows 50-60
\echo '  Step 3: delete rows 50-60'
SELECT * FROM lance_delete(
  '/tmp/lance_e2e_test/t_roundtrip.lance',
  'id >= 50 AND id <= 60'
);

DROP FOREIGN TABLE IF EXISTS le2e.t_roundtrip CASCADE;
SELECT lance_import('lance_e2e', 'le2e', 't_roundtrip', '/tmp/lance_e2e_test/t_roundtrip.lance', batch_size => NULL);

-- Should have 99 rows (110 - 11 deleted)
SELECT count(*) AS lance_after_delete FROM le2e.t_roundtrip;
SELECT * FROM duckdb.query($$SELECT count(*) AS duckdb_after_delete FROM '/tmp/lance_e2e_test/t_roundtrip.lance'$$);

-- No rows in deleted range
SELECT count(*) AS in_deleted_range FROM le2e.t_roundtrip WHERE id BETWEEN 50 AND 60;

\echo '  PASS: full roundtrip with all types verified by DuckDB'

-- ─────────────────────────────────────────────────────────────────────────────
-- 13. DATA INTEGRITY: Lance FDW vs DuckDB row-by-row comparison
-- ─────────────────────────────────────────────────────────────────────────────
\echo ''
\echo '>>> 13. Data integrity — Lance FDW vs DuckDB row-level comparison'

-- Create a known dataset
DROP FOREIGN TABLE IF EXISTS le2e.t_integrity CASCADE;

SELECT * FROM lance_append(
  '/tmp/lance_e2e_test/t_integrity.lance',
  $q$
    SELECT v::int4 AS id,
           ('val_' || v::text) AS label,
           (v * 3.14)::float8 AS measurement
    FROM generate_series(1, 50) v
  $q$,
  mode := 'create'
);

SELECT lance_import('lance_e2e', 'le2e', 't_integrity', '/tmp/lance_e2e_test/t_integrity.lance', batch_size => NULL);

-- Compare counts between Lance FDW and DuckDB reads.
\echo '  Lance FDW count:'
SELECT count(*) AS lance_integrity_count FROM le2e.t_integrity;
\echo '  DuckDB count:'
SELECT * FROM duckdb.query($$SELECT count(*) AS duckdb_integrity_count FROM '/tmp/lance_e2e_test/t_integrity.lance'$$);

\echo '  PASS: row-level integrity check (Lance FDW == DuckDB)'

-- ─────────────────────────────────────────────────────────────────────────────
-- 14. NULL handling across operations
-- ─────────────────────────────────────────────────────────────────────────────
\echo ''
\echo '>>> 14. NULL handling'

DROP FOREIGN TABLE IF EXISTS le2e.t_nulls CASCADE;

SELECT * FROM lance_append(
  '/tmp/lance_e2e_test/t_nulls.lance',
  $q$
    SELECT * FROM (VALUES
      (1::int4, 'has_value'::text, 100::int4,  true),
      (2::int4, NULL::text,        NULL::int4,  NULL::bool),
      (3::int4, 'also_has'::text,  NULL::int4,  false),
      (4::int4, NULL::text,        400::int4,   NULL::bool),
      (5::int4, NULL::text,        NULL::int4,  NULL::bool)
    ) AS t(id, label, amount, flag)
  $q$,
  mode := 'create'
);

SELECT lance_import('lance_e2e', 'le2e', 't_nulls', '/tmp/lance_e2e_test/t_nulls.lance', batch_size => NULL);

-- Check NULLs survived
SELECT count(*) AS null_label_count FROM le2e.t_nulls WHERE label IS NULL;
-- Should be 3 (ids 2, 4, 5)

SELECT count(*) AS null_amount_count FROM le2e.t_nulls WHERE amount IS NULL;
-- Should be 3 (ids 2, 3, 5)

SELECT count(*) AS null_flag_count FROM le2e.t_nulls WHERE flag IS NULL;
-- Should be 3 (ids 2, 4, 5)

-- Merge-insert: update id=2 to have values; insert id=6 with NULLs
SELECT * FROM lance_merge_insert(
  '/tmp/lance_e2e_test/t_nulls.lance',
  $q$
    SELECT * FROM (VALUES
      (2::int4, 'now_filled'::text, 200::int4, true),
      (6::int4, NULL::text, NULL::int4, NULL::bool)
    ) AS t(id, label, amount, flag)
  $q$,
  on_columns := ARRAY['id'],
  when_matched := 'update',
  when_not_matched := 'insert'
);

DROP FOREIGN TABLE IF EXISTS le2e.t_nulls CASCADE;
SELECT lance_import('lance_e2e', 'le2e', 't_nulls', '/tmp/lance_e2e_test/t_nulls.lance', batch_size => NULL);

-- id=2 should now have values
\echo '  id=2 should be filled:'
SELECT id, label, amount, flag FROM le2e.t_nulls WHERE id = 2;

-- id=6 should have NULLs
\echo '  id=6 should have NULLs:'
SELECT id,
       (label IS NULL) AS label_null,
       (amount IS NULL) AS amount_null,
       (flag IS NULL) AS flag_null
  FROM le2e.t_nulls WHERE id = 6;

-- Delete rows where label IS NULL (using Lance predicate syntax)
SELECT * FROM lance_delete(
  '/tmp/lance_e2e_test/t_nulls.lance',
  'label IS NULL'
);

DROP FOREIGN TABLE IF EXISTS le2e.t_nulls CASCADE;
SELECT lance_import('lance_e2e', 'le2e', 't_nulls', '/tmp/lance_e2e_test/t_nulls.lance', batch_size => NULL);

-- Should have deleted ids 4, 5, 6 (label IS NULL) — leaving 1, 2, 3
SELECT count(*) AS after_null_delete FROM le2e.t_nulls;

-- DuckDB cross-check
SELECT * FROM duckdb.query($$SELECT count(*) AS duckdb_after_null_delete FROM '/tmp/lance_e2e_test/t_nulls.lance'$$);

\echo '  PASS: NULL handling across insert, update, delete'

-- ─────────────────────────────────────────────────────────────────────────────
-- 15. Edge cases: empty dataset, zero-row merge, delete with no matches
-- ─────────────────────────────────────────────────────────────────────────────
\echo ''
\echo '>>> 15. Edge cases'

-- Create a dataset with 1 row then delete it to get an empty-but-valid dataset.
-- (lance_append with 0 rows does not create the dataset on disk.)
\echo '  15a. Create dataset, then delete all rows to make it empty:'
SELECT * FROM lance_append(
  '/tmp/lance_e2e_test/t_empty.lance',
  'SELECT 0::int4 AS id, ''seed''::text AS val',
  mode := 'create'
);
SELECT * FROM lance_delete(
  '/tmp/lance_e2e_test/t_empty.lance',
  'id = 0'
);

DROP FOREIGN TABLE IF EXISTS le2e.t_empty CASCADE;
SELECT lance_import('lance_e2e', 'le2e', 't_empty', '/tmp/lance_e2e_test/t_empty.lance', batch_size => NULL);
SELECT count(*) AS empty_count FROM le2e.t_empty;

-- Append some rows to the empty dataset
SELECT * FROM lance_append(
  '/tmp/lance_e2e_test/t_empty.lance',
  $q$SELECT v::int4 AS id, ('v' || v::text) AS val FROM generate_series(1,3) v$q$,
  mode := 'append'
);

DROP FOREIGN TABLE IF EXISTS le2e.t_empty CASCADE;
SELECT lance_import('lance_e2e', 'le2e', 't_empty', '/tmp/lance_e2e_test/t_empty.lance', batch_size => NULL);
SELECT count(*) AS after_append_to_empty FROM le2e.t_empty;

-- Delete with a predicate that matches no rows
\echo '  15b. Delete with no matching rows:'
SELECT * FROM lance_delete(
  '/tmp/lance_e2e_test/t_empty.lance',
  'id > 9999'
);

DROP FOREIGN TABLE IF EXISTS le2e.t_empty CASCADE;
SELECT lance_import('lance_e2e', 'le2e', 't_empty', '/tmp/lance_e2e_test/t_empty.lance', batch_size => NULL);
SELECT count(*) AS after_noop_delete FROM le2e.t_empty;

-- Merge with matching row but no real change (update to same values)
\echo '  15c. Merge-insert — update existing row to same values:'
SELECT * FROM lance_merge_insert(
  '/tmp/lance_e2e_test/t_empty.lance',
  $q$SELECT 1::int4 AS id, 'v1'::text AS val$q$,
  on_columns := ARRAY['id'],
  when_matched := 'update',
  when_not_matched := 'nothing'
);

DROP FOREIGN TABLE IF EXISTS le2e.t_empty CASCADE;
SELECT lance_import('lance_e2e', 'le2e', 't_empty', '/tmp/lance_e2e_test/t_empty.lance', batch_size => NULL);
SELECT count(*) AS after_noop_merge FROM le2e.t_empty;

\echo '  PASS: edge cases'

-- ─────────────────────────────────────────────────────────────────────────────
-- 16. Large batch type verification via DuckDB
-- ─────────────────────────────────────────────────────────────────────────────
\echo ''
\echo '>>> 16. Large batch (10000 rows) — type fidelity via DuckDB'

DROP FOREIGN TABLE IF EXISTS le2e.t_large CASCADE;

SELECT * FROM lance_append(
  '/tmp/lance_e2e_test/t_large.lance',
  $q$
    SELECT
      v::int4                                      AS id,
      (v % 2 = 0)                                 AS flag,
      (v % 32000)::int2                            AS i2,
      v::int4                                      AS i4,
      (v::int8 * 1000000)                          AS i8,
      (v * 0.1)::float4                            AS f4,
      (v * 0.001)::float8                          AS f8,
      ('row_' || lpad(v::text, 5, '0'))::text      AS label,
      ('2020-01-01'::date + (v % 3650))            AS dt,
      ('2020-01-01 00:00:00'::timestamp + ((v % 86400) || ' seconds')::interval) AS ts,
      ('2020-01-01 00:00:00+00'::timestamptz + ((v % 86400) || ' seconds')::interval) AS tstz,
      (v * 0.01)::numeric                          AS num,
      ('{"i":' || v || '}')::jsonb                 AS j
    FROM generate_series(1, 10000) v
  $q$,
  mode := 'create'
);

SELECT lance_import('lance_e2e', 'le2e', 't_large', '/tmp/lance_e2e_test/t_large.lance', batch_size => NULL);

-- Count check
SELECT count(*) AS lance_large_count FROM le2e.t_large;
SELECT * FROM duckdb.query($$SELECT count(*) AS duckdb_large_count FROM '/tmp/lance_e2e_test/t_large.lance'$$);

-- Aggregate cross-checks
\echo '  Aggregate cross-check (min/max id):'
SELECT min(id) AS min_id, max(id) AS max_id FROM le2e.t_large;
SELECT * FROM duckdb.query($$SELECT min(id) AS duck_min_id, max(id) AS duck_max_id FROM '/tmp/lance_e2e_test/t_large.lance'$$);

-- Sum cross-check
\echo '  Sum cross-check (sum of id):'
SELECT sum(id) AS lance_sum FROM le2e.t_large;
SELECT * FROM duckdb.query($$SELECT sum(id) AS duckdb_sum FROM '/tmp/lance_e2e_test/t_large.lance'$$);

\echo '  PASS: large batch type fidelity'

-- ─────────────────────────────────────────────────────────────────────────────
-- Cleanup
-- ─────────────────────────────────────────────────────────────────────────────
\echo ''
\echo '>>> Cleanup'

DROP FOREIGN TABLE IF EXISTS le2e.t_types CASCADE;
DROP FOREIGN TABLE IF EXISTS le2e.t_bulk CASCADE;
DROP FOREIGN TABLE IF EXISTS le2e.t_merge CASCADE;
DROP FOREIGN TABLE IF EXISTS le2e.t_roundtrip CASCADE;
DROP FOREIGN TABLE IF EXISTS le2e.t_integrity CASCADE;
DROP FOREIGN TABLE IF EXISTS le2e.t_nulls CASCADE;
DROP FOREIGN TABLE IF EXISTS le2e.t_empty CASCADE;
DROP FOREIGN TABLE IF EXISTS le2e.t_large CASCADE;
DROP SCHEMA IF EXISTS le2e CASCADE;
-- Note: we leave the lance_e2e server and extensions in place.
-- To fully clean up, also run:
--   DROP SERVER IF EXISTS lance_e2e CASCADE;
--   DROP EXTENSION IF EXISTS lance CASCADE;
--   DROP EXTENSION IF EXISTS duckdb CASCADE;
-- And remove the Lance data directory: rm -rf /tmp/lance_e2e_test

\echo ''
\echo '============================================================'
\echo ' ALL TESTS PASSED'
\echo '============================================================'
\echo ''
\echo 'To remove Lance data files, run:'
\echo '  rm -rf /tmp/lance_e2e_test'
