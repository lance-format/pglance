-- =============================================================================
-- Benchmark: pglance-write — Lance-only INSERT / UPDATE / DELETE
-- =============================================================================
--
-- NOTE: The primary benchmark is run_benchmark.sh which compares Lance vs
--       PostgreSQL with nice formatted output. This SQL file is a simpler
--       Lance-only alternative you can run directly via:
--
--         psql -f benchmark_write.sql
--
-- Lance datasets written to /tmp/lance_bench.
--
-- Tests:
--   A. 100 individual inserts / updates / deletes  (row-at-a-time)
--   B. Bulk 100K insert / update / delete
--   C. Bulk 1M  insert / update / delete
-- =============================================================================

\set ON_ERROR_STOP on
\timing on

-- Clean up leftovers from previous runs
\! rm -rf /tmp/lance_bench

\echo '============================================================'
\echo ' pglance-write Benchmark'
\echo ' Lance dataset directory: /tmp/lance_bench'
\echo '============================================================'

-- ─────────────────────────────────────────────────────────────────────────────
-- 0. Setup
-- ─────────────────────────────────────────────────────────────────────────────
\echo ''
\echo '>>> 0. Setup'

CREATE EXTENSION IF NOT EXISTS lance;

DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_foreign_server WHERE srvname = 'lance_bench') THEN
    EXECUTE 'CREATE SERVER lance_bench FOREIGN DATA WRAPPER lance_fdw';
  END IF;
END $$;

CREATE SCHEMA IF NOT EXISTS lbench;

\echo '  ... setup complete'

-- ─────────────────────────────────────────────────────────────────────────────
-- 0b. Generate source data into UNLOGGED tables (cached in memory)
-- ─────────────────────────────────────────────────────────────────────────────
\echo ''
\echo '>>> 0b. Generate source data'

DROP TABLE IF EXISTS bench_src_100  CASCADE;
DROP TABLE IF EXISTS bench_src_100k CASCADE;
DROP TABLE IF EXISTS bench_src_1m   CASCADE;

CREATE UNLOGGED TABLE bench_src_100 (
    id int4 PRIMARY KEY, name text NOT NULL, score float8 NOT NULL,
    active bool NOT NULL, created date NOT NULL
);
INSERT INTO bench_src_100
SELECT v::int4, 'item_'||v, (v*1.1)::float8, (v%2=0), '2025-01-01'::date+(v%365)
FROM generate_series(1,100) v;
ANALYZE bench_src_100;
SELECT count(*) FROM bench_src_100;  -- warm cache

CREATE UNLOGGED TABLE bench_src_100k (
    id int4 PRIMARY KEY, name text NOT NULL, score float8 NOT NULL,
    active bool NOT NULL, created date NOT NULL
);
INSERT INTO bench_src_100k
SELECT v::int4, 'item_'||v, (v*1.1)::float8, (v%2=0), '2025-01-01'::date+(v%365)
FROM generate_series(1,100000) v;
ANALYZE bench_src_100k;
SELECT count(*) FROM bench_src_100k;  -- warm cache

CREATE UNLOGGED TABLE bench_src_1m (
    id int4 PRIMARY KEY, name text NOT NULL, score float8 NOT NULL,
    active bool NOT NULL, created date NOT NULL
);
INSERT INTO bench_src_1m
SELECT v::int4, 'item_'||v, (v*1.1)::float8, (v%2=0), '2025-01-01'::date+(v%365)
FROM generate_series(1,1000000) v;
ANALYZE bench_src_1m;
SELECT count(*) FROM bench_src_1m;  -- warm cache

\echo '  ... source data ready'

-- =============================================================================
-- A. 100 INDIVIDUAL ROW OPERATIONS
-- =============================================================================

-- ─────────────────────────────────────────────────────────────────────────────
-- A1. 100 individual INSERTs (one lance_append per row)
-- ─────────────────────────────────────────────────────────────────────────────
\echo ''
\echo '>>> A1. 100 individual inserts'

-- Seed the dataset with a single row so it exists
SELECT * FROM lance_append(
  '/tmp/lance_bench/individual.lance',
  $q$
    SELECT
      0::int4            AS id,
      'seed'::text       AS name,
      0.0::float8        AS score,
      true               AS active,
      '2025-01-01'::date AS created
  $q$,
  mode := 'create'
);

DO $$
BEGIN
  FOR i IN 1..100 LOOP
    PERFORM * FROM lance_append(
      '/tmp/lance_bench/individual.lance',
      format(
        'SELECT %s::int4 AS id, %L::text AS name, %s::float8 AS score, %L::bool AS active, %L::date AS created',
        i, 'item_' || i, i * 1.1, (i % 2 = 0), '2025-01-01'::date + i
      ),
      mode := 'append'
    );
  END LOOP;
END $$;

-- Verify: should have 101 rows (seed + 100)
DROP FOREIGN TABLE IF EXISTS lbench.individual CASCADE;
SELECT lance_import('lance_bench', 'lbench', 'individual', '/tmp/lance_bench/individual.lance', batch_size => NULL);
SELECT count(*) AS individual_insert_count FROM lbench.individual;

\echo '  PASS: 100 individual inserts'

-- ─────────────────────────────────────────────────────────────────────────────
-- A2. 100 individual UPDATEs (one lance_merge_insert per row)
-- ─────────────────────────────────────────────────────────────────────────────
\echo ''
\echo '>>> A2. 100 individual updates'

DO $$
BEGIN
  FOR i IN 1..100 LOOP
    PERFORM * FROM lance_merge_insert(
      '/tmp/lance_bench/individual.lance',
      format(
        'SELECT %s::int4 AS id, %L::text AS name, %s::float8 AS score, %L::bool AS active, %L::date AS created',
        i, 'updated_' || i, i * 99.9, NOT (i % 2 = 0), '2026-06-01'::date
      ),
      on_columns := ARRAY['id'],
      when_matched := 'update',
      when_not_matched := 'nothing'
    );
  END LOOP;
END $$;

-- Verify: still 101 rows, spot-check updated values
DROP FOREIGN TABLE IF EXISTS lbench.individual CASCADE;
SELECT lance_import('lance_bench', 'lbench', 'individual', '/tmp/lance_bench/individual.lance', batch_size => NULL);
SELECT count(*) AS individual_update_count FROM lbench.individual;
SELECT id, name, score FROM lbench.individual WHERE id = 50;

\echo '  PASS: 100 individual updates'

-- ─────────────────────────────────────────────────────────────────────────────
-- A3. 100 individual DELETEs (one lance_delete per row)
-- ─────────────────────────────────────────────────────────────────────────────
\echo ''
\echo '>>> A3. 100 individual deletes'

DO $$
BEGIN
  FOR i IN 1..100 LOOP
    PERFORM * FROM lance_delete(
      '/tmp/lance_bench/individual.lance',
      format('id = %s', i)
    );
  END LOOP;
END $$;

-- Verify: only the seed row (id=0) should remain
DROP FOREIGN TABLE IF EXISTS lbench.individual CASCADE;
SELECT lance_import('lance_bench', 'lbench', 'individual', '/tmp/lance_bench/individual.lance', batch_size => NULL);
SELECT count(*) AS individual_delete_count FROM lbench.individual;

\echo '  PASS: 100 individual deletes'

-- =============================================================================
-- B. BULK 100K OPERATIONS
-- =============================================================================

-- ─────────────────────────────────────────────────────────────────────────────
-- B1. Bulk INSERT 100K rows
-- ─────────────────────────────────────────────────────────────────────────────
\echo ''
\echo '>>> B1. Bulk insert — 100K rows'

SELECT * FROM lance_append(
  '/tmp/lance_bench/bulk_100k.lance',
  $q$
    SELECT id, name, score, active, created
    FROM bench_src_100k
  $q$,
  mode := 'create'
);

-- Verify
DROP FOREIGN TABLE IF EXISTS lbench.bulk_100k CASCADE;
SELECT lance_import('lance_bench', 'lbench', 'bulk_100k', '/tmp/lance_bench/bulk_100k.lance', batch_size => NULL);
SELECT count(*) AS bulk_100k_insert_count FROM lbench.bulk_100k;

\echo '  PASS: bulk insert 100K rows'

-- ─────────────────────────────────────────────────────────────────────────────
-- B2. Bulk UPDATE 100K rows (merge_insert, update all)
-- ─────────────────────────────────────────────────────────────────────────────
\echo ''
\echo '>>> B2. Bulk update — 100K rows'

SELECT * FROM lance_merge_insert(
  '/tmp/lance_bench/bulk_100k.lance',
  $q$
    SELECT id,
           'updated_' || id::text AS name,
           (id * 99.9)::float8    AS score,
           NOT active             AS active,
           '2026-06-01'::date     AS created
    FROM bench_src_100k
  $q$,
  on_columns := ARRAY['id'],
  when_matched := 'update',
  when_not_matched := 'nothing'
);

-- Verify: still 100K rows, spot-check
DROP FOREIGN TABLE IF EXISTS lbench.bulk_100k CASCADE;
SELECT lance_import('lance_bench', 'lbench', 'bulk_100k', '/tmp/lance_bench/bulk_100k.lance', batch_size => NULL);
SELECT count(*) AS bulk_100k_update_count FROM lbench.bulk_100k;
SELECT id, name FROM lbench.bulk_100k WHERE id = 50000;

\echo '  PASS: bulk update 100K rows'

-- ─────────────────────────────────────────────────────────────────────────────
-- B3. Bulk DELETE 100K rows
-- ─────────────────────────────────────────────────────────────────────────────
\echo ''
\echo '>>> B3. Bulk delete — 100K rows'

SELECT * FROM lance_delete(
  '/tmp/lance_bench/bulk_100k.lance',
  'id >= 1 AND id <= 100000'
);

-- Verify: 0 rows remaining
DROP FOREIGN TABLE IF EXISTS lbench.bulk_100k CASCADE;
SELECT lance_import('lance_bench', 'lbench', 'bulk_100k', '/tmp/lance_bench/bulk_100k.lance', batch_size => NULL);
SELECT count(*) AS bulk_100k_delete_count FROM lbench.bulk_100k;

\echo '  PASS: bulk delete 100K rows'

-- =============================================================================
-- C. BULK 1M OPERATIONS
-- =============================================================================

-- ─────────────────────────────────────────────────────────────────────────────
-- C1. Bulk INSERT 1M rows
-- ─────────────────────────────────────────────────────────────────────────────
\echo ''
\echo '>>> C1. Bulk insert — 1M rows'

SELECT * FROM lance_append(
  '/tmp/lance_bench/bulk_1m.lance',
  $q$
    SELECT id, name, score, active, created
    FROM bench_src_1m
  $q$,
  mode := 'create'
);

-- Verify
DROP FOREIGN TABLE IF EXISTS lbench.bulk_1m CASCADE;
SELECT lance_import('lance_bench', 'lbench', 'bulk_1m', '/tmp/lance_bench/bulk_1m.lance', batch_size => NULL);
SELECT count(*) AS bulk_1m_insert_count FROM lbench.bulk_1m;

\echo '  PASS: bulk insert 1M rows'

-- ─────────────────────────────────────────────────────────────────────────────
-- C2. Bulk UPDATE 1M rows (merge_insert, update all)
-- ─────────────────────────────────────────────────────────────────────────────
\echo ''
\echo '>>> C2. Bulk update — 1M rows'

SELECT * FROM lance_merge_insert(
  '/tmp/lance_bench/bulk_1m.lance',
  $q$
    SELECT id,
           'updated_' || id::text AS name,
           (id * 99.9)::float8    AS score,
           NOT active             AS active,
           '2026-06-01'::date     AS created
    FROM bench_src_1m
  $q$,
  on_columns := ARRAY['id'],
  when_matched := 'update',
  when_not_matched := 'nothing'
);

-- Verify: still 1M rows, spot-check
DROP FOREIGN TABLE IF EXISTS lbench.bulk_1m CASCADE;
SELECT lance_import('lance_bench', 'lbench', 'bulk_1m', '/tmp/lance_bench/bulk_1m.lance', batch_size => NULL);
SELECT count(*) AS bulk_1m_update_count FROM lbench.bulk_1m;
SELECT id, name FROM lbench.bulk_1m WHERE id = 500000;

\echo '  PASS: bulk update 1M rows'

-- ─────────────────────────────────────────────────────────────────────────────
-- C3. Bulk DELETE 1M rows
-- ─────────────────────────────────────────────────────────────────────────────
\echo ''
\echo '>>> C3. Bulk delete — 1M rows'

SELECT * FROM lance_delete(
  '/tmp/lance_bench/bulk_1m.lance',
  'id >= 1 AND id <= 1000000'
);

-- Verify: 0 rows remaining
DROP FOREIGN TABLE IF EXISTS lbench.bulk_1m CASCADE;
SELECT lance_import('lance_bench', 'lbench', 'bulk_1m', '/tmp/lance_bench/bulk_1m.lance', batch_size => NULL);
SELECT count(*) AS bulk_1m_delete_count FROM lbench.bulk_1m;

\echo '  PASS: bulk delete 1M rows'

-- =============================================================================
-- CLEANUP
-- =============================================================================
\echo ''
\echo '>>> Cleanup'

DROP FOREIGN TABLE IF EXISTS lbench.individual  CASCADE;
DROP FOREIGN TABLE IF EXISTS lbench.bulk_100k   CASCADE;
DROP FOREIGN TABLE IF EXISTS lbench.bulk_1m     CASCADE;
DROP TABLE IF EXISTS bench_src_100  CASCADE;
DROP TABLE IF EXISTS bench_src_100k CASCADE;
DROP TABLE IF EXISTS bench_src_1m   CASCADE;
DROP SCHEMA IF EXISTS lbench CASCADE;
DROP SERVER IF EXISTS lance_bench CASCADE;

\! rm -rf /tmp/lance_bench

\echo ''
\echo '============================================================'
\echo ' Benchmark complete — all tests passed'
\echo '============================================================'
