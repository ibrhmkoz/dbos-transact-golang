-- Workspace database schema: views over the immutable per-run parquet files.
-- __TEST_RESULTS_ROOT__ is replaced with the absolute .test-results path by
-- record-tests.sh when it creates the workspace database. Globs are expanded
-- at query time, so new runs appear without reconnecting.

CREATE OR REPLACE VIEW test_runs AS
SELECT *
FROM read_parquet('__TEST_RESULTS_ROOT__/runs/*/runs.parquet', union_by_name = true);

CREATE OR REPLACE VIEW test_events AS
SELECT *
FROM read_parquet('__TEST_RESULTS_ROOT__/runs/*/events.parquet', union_by_name = true);

CREATE OR REPLACE VIEW test_results AS
SELECT *
FROM read_parquet('__TEST_RESULTS_ROOT__/runs/*/results.parquet', union_by_name = true);

CREATE OR REPLACE VIEW latest_test_run AS
SELECT *
FROM test_runs
ORDER BY started_at DESC
LIMIT 1;

-- Leaf tests only: a parent test groups subtests and is excluded.
CREATE OR REPLACE VIEW leaf_test_results AS
SELECT results.*
FROM test_results AS results
WHERE NOT EXISTS (
    SELECT 1
    FROM test_results AS child
    WHERE child.run_id = results.run_id
      AND child.package = results.package
      AND starts_with(child.test, results.test || '/')
);

CREATE OR REPLACE VIEW latest_test_results AS
SELECT results.*
FROM leaf_test_results AS results
JOIN latest_test_run AS latest USING (run_id);

CREATE OR REPLACE VIEW failed_test_results AS
SELECT runs.started_at, runs.backend, runs.race, results.*
FROM leaf_test_results AS results
JOIN test_runs AS runs USING (run_id)
WHERE results.status = 'fail';
