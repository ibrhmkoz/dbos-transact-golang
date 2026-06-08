-- Recent test runs.
SELECT *
FROM test_runs
ORDER BY started_at DESC
LIMIT 20;

-- Latest run wall time compared with the sum of leaf-test elapsed times.
-- Parent tests are excluded because their elapsed time includes their subtests.
SELECT
    runs.duration_seconds AS wall_seconds,
    phases.wall_seconds AS parallel_phase_wall_seconds,
    round(sum(results.elapsed_seconds) FILTER (WHERE NOT results.parallel), 3) AS serial_leaf_seconds,
    round(sum(results.elapsed_seconds) FILTER (WHERE results.parallel), 3) AS parallel_work_seconds,
    round(max(results.elapsed_seconds) FILTER (WHERE results.parallel), 3) AS longest_parallel_test_seconds,
    round(sum(results.elapsed_seconds) / runs.duration_seconds, 2) AS effective_parallelism,
    count(*) AS leaf_tests
FROM latest_test_run AS runs
CROSS JOIN latest_leaf_test_results AS results
CROSS JOIN latest_parallel_phase AS phases
GROUP BY runs.duration_seconds, phases.wall_seconds;

-- Leaf-test work split by serial and parallel execution.
-- Parallel work_seconds is not wall time because tests overlap.
SELECT *
FROM latest_test_parallelism;

-- Slowest serial leaf tests. These directly extend wall time.
SELECT test, package, elapsed_seconds
FROM latest_leaf_test_results
WHERE NOT parallel
ORDER BY elapsed_seconds DESC
LIMIT 30;

-- Slowest parallel leaf tests. These may define the parallel phase's critical path.
SELECT test, package, elapsed_seconds
FROM latest_leaf_test_results
WHERE parallel
ORDER BY elapsed_seconds DESC
LIMIT 30;

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

-- Slowest tests in this recorded run.
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
--   duckdb -readonly .test-results/latest.duckdb -f scripts/test-results.sql
SELECT string_agg(events.output, '' ORDER BY events.event_index) AS output
FROM latest_test_events AS events
WHERE events.test = getenv('TEST_NAME')
  AND events.output IS NOT NULL;
