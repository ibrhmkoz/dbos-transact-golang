-- Recent test runs.
SELECT *
FROM test_runs
ORDER BY started_at DESC
LIMIT 20;

-- Latest run wall time compared with the sum of leaf-test elapsed times.
SELECT
    runs.duration_seconds AS wall_seconds,
    round(sum(results.elapsed_seconds) FILTER (WHERE NOT results.parallel), 3) AS serial_leaf_seconds,
    round(sum(results.elapsed_seconds) FILTER (WHERE results.parallel), 3) AS parallel_work_seconds,
    round(max(results.elapsed_seconds) FILTER (WHERE results.parallel), 3) AS longest_parallel_test_seconds,
    round(sum(results.elapsed_seconds) / runs.duration_seconds, 2) AS effective_parallelism,
    count(*) AS leaf_tests
FROM latest_test_run AS runs
CROSS JOIN latest_test_results AS results
GROUP BY runs.duration_seconds;

-- Slowest serial leaf tests. These directly extend wall time.
SELECT test, package, elapsed_seconds
FROM latest_test_results
WHERE NOT parallel
ORDER BY elapsed_seconds DESC
LIMIT 30;

-- Slowest parallel leaf tests. These may define the parallel phase's critical path.
SELECT test, package, elapsed_seconds
FROM latest_test_results
WHERE parallel
ORDER BY elapsed_seconds DESC
LIMIT 30;

-- Failed tests from the latest run (deepest failing nodes), with their output.
SELECT failed.test, failed.package, failed.elapsed_seconds, failed.output
FROM failed_test_results AS failed
JOIN latest_test_run AS latest USING (run_id)
ORDER BY failed.elapsed_seconds DESC;

-- Failed packages from the latest run (setup/build failures have no test rows).
SELECT events.package, events.elapsed_seconds
FROM test_events AS events
JOIN latest_test_run AS latest USING (run_id)
WHERE events.test IS NULL
  AND events.package IS NOT NULL
  AND events.action = 'fail'
ORDER BY events.elapsed_seconds DESC;

-- Output for one test from the latest run.
-- Usage:
-- TEST_NAME='TestGarbageCollect/GarbageCollectOnlyCompletedWorkflows' \
--   duckdb -readonly .test-results/results.duckdb -f scripts/test-results.sql
SELECT output
FROM latest_test_results
WHERE test = getenv('TEST_NAME');
