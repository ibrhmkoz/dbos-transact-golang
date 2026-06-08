-- Recent test runs.
SELECT *
FROM test_runs
ORDER BY started_at DESC
LIMIT 20;

-- Failed tests from the latest run.
SELECT test, package, elapsed_seconds
FROM latest_test_results
WHERE status = 'fail'
ORDER BY elapsed_seconds DESC;

-- Failed packages from the latest run, including setup/build failures.
SELECT package, elapsed_seconds
FROM latest_package_results
WHERE status = 'fail'
ORDER BY elapsed_seconds DESC;

-- Slowest tests across all recorded runs.
SELECT
    test,
    package,
    runs,
    round(avg_seconds, 3) AS avg_seconds,
    round(p95_seconds, 3) AS p95_seconds,
    round(max_seconds, 3) AS max_seconds
FROM test_duration_history
ORDER BY avg_seconds DESC
LIMIT 30;

-- Failure output from the latest run.
SELECT events.test, events.package, events.output
FROM latest_test_events AS events
LEFT JOIN latest_test_results AS results USING (run_id, package, test)
WHERE events.output IS NOT NULL
  AND (results.status = 'fail' OR events.action = 'build-output')
ORDER BY events.event_index;

-- Output for one test from the latest run.
-- Usage:
-- TEST_NAME='TestGarbageCollect/GarbageCollectOnlyCompletedWorkflows' \
--   duckdb .test-results/tests.duckdb -f scripts/test-results.sql
SELECT string_agg(events.output, '' ORDER BY events.event_index) AS output
FROM latest_test_events AS events
WHERE events.test = 'TestGarbageCollect/GarbageCollectOnlyCompletedWorkflows'
  AND events.output IS NOT NULL;
